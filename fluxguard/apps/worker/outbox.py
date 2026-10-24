import json
import signal
import time
from datetime import timedelta
from threading import Event
from typing import Protocol

from confluent_kafka import Producer
from sqlalchemy import or_, select
from sqlalchemy.orm import Session, sessionmaker

from fluxguard.packages.common.config import Settings, get_settings
from fluxguard.packages.common.db import SessionLocal
from fluxguard.packages.common.logging import configure_logging, event_id_var, get_logger
from fluxguard.packages.common.metrics import counters
from fluxguard.packages.common.models import OutboxEvent
from fluxguard.packages.common.time import utc_now

logger = get_logger(__name__)


class EventPublisher(Protocol):
    def publish(self, topic: str, key: str, value: bytes) -> None: ...


class KafkaEventPublisher:
    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "client.id": "fluxguard-outbox",
            }
        )

    def publish(self, topic: str, key: str, value: bytes) -> None:
        delivery_error: list[Exception] = []

        def delivered(error, _message) -> None:
            if error is not None:
                delivery_error.append(RuntimeError(str(error)))

        self._producer.produce(topic=topic, key=key, value=value, on_delivery=delivered)
        remaining = self._producer.flush(10)
        if delivery_error:
            raise delivery_error[0]
        if remaining:
            raise TimeoutError(f"{remaining} Kafka message(s) were not acknowledged")


def event_bytes(event: OutboxEvent) -> bytes:
    envelope = {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "schema_version": event.schema_version,
        "aggregate_id": str(event.aggregate_id),
        "occurred_at": event.occurred_at.isoformat(),
        "payload": event.payload,
    }
    return json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode()


def publish_pending(
    session_factory: sessionmaker[Session],
    publisher: EventPublisher,
    topic: str,
    batch_size: int = 50,
) -> int:
    published = 0
    now = utc_now()
    with session_factory() as session, session.begin():
        events = session.scalars(
            select(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                or_(
                    OutboxEvent.next_attempt_at.is_(None),
                    OutboxEvent.next_attempt_at <= now,
                ),
            )
            .order_by(OutboxEvent.occurred_at)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        ).all()
        for event in events:
            token = event_id_var.set(str(event.event_id))
            try:
                publisher.publish(topic, str(event.aggregate_id), event_bytes(event))
                event.published_at = utc_now()
                event.attempts += 1
                event.last_error = None
                published += 1
                counters.increment("outbox_published")
                logger.info("outbox_event_published", aggregate_id=str(event.aggregate_id))
            except Exception as exc:
                event.attempts += 1
                delay = min(60, 2 ** min(event.attempts, 6))
                event.next_attempt_at = utc_now() + timedelta(seconds=delay)
                event.last_error = str(exc)[:4000]
                counters.increment("outbox_publish_failed")
                logger.warning(
                    "outbox_publish_failed",
                    aggregate_id=str(event.aggregate_id),
                    error=str(exc),
                    retry_seconds=delay,
                )
                break
            finally:
                event_id_var.reset(token)
    return published


def run(settings: Settings, stop: Event | None = None) -> None:
    stop = stop or Event()
    publisher = KafkaEventPublisher(settings.kafka_bootstrap_servers)
    logger.info("outbox_worker_started", topic=settings.kafka_orders_topic)
    while not stop.is_set():
        count = publish_pending(
            SessionLocal,
            publisher,
            settings.kafka_orders_topic,
            settings.outbox_batch_size,
        )
        if count == 0:
            stop.wait(settings.outbox_poll_interval_seconds)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    stop = Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    while not stop.is_set():
        try:
            run(settings, stop)
        except Exception:
            logger.exception("outbox_worker_cycle_failed")
            counters.increment("outbox_worker_failed")
            time.sleep(min(5.0, settings.outbox_poll_interval_seconds))


if __name__ == "__main__":
    main()
