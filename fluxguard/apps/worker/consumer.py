import base64
import json
import signal
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from threading import Event
from typing import Protocol

from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, sessionmaker

from fluxguard.packages.common.config import Settings, get_settings
from fluxguard.packages.common.db import SessionLocal
from fluxguard.packages.common.logging import configure_logging, event_id_var, get_logger
from fluxguard.packages.common.metrics import counters
from fluxguard.packages.common.models import (
    OrderProjection,
    ProcessedEvent,
    QuarantinedEvent,
)
from fluxguard.packages.contracts.events import OrderCreatedEvent

logger = get_logger(__name__)
MAX_QUARANTINE_PAYLOAD_BYTES = 1_000_000


class PermanentEventError(Exception):
    pass


@dataclass(frozen=True)
class EventRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


class CommittableConsumer(Protocol):
    def poll(self, timeout: float): ...

    def commit(self, message, asynchronous: bool): ...

    def subscribe(self, topics: list[str]): ...

    def close(self): ...


def safe_text(value: bytes | None) -> str | None:
    if value is None:
        return None
    clipped = value[:MAX_QUARANTINE_PAYLOAD_BYTES]
    try:
        return clipped.decode("utf-8")
    except UnicodeDecodeError:
        return "base64:" + base64.b64encode(clipped).decode("ascii")


def parse_event(raw: bytes | None) -> OrderCreatedEvent:
    if raw is None:
        raise PermanentEventError("event payload is null")
    try:
        decoded = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PermanentEventError(f"invalid JSON: {exc}") from exc
    try:
        event = OrderCreatedEvent.model_validate(decoded)
    except ValidationError as exc:
        raise PermanentEventError(f"contract validation failed: {exc}") from exc
    if event.aggregate_id != event.payload.order_id:
        raise PermanentEventError("aggregate_id does not match payload.order_id")
    calculated = sum((item.line_total for item in event.payload.items), start=Decimal("0.00"))
    if calculated != event.payload.total:
        raise PermanentEventError("payload total does not equal its line totals")
    for item in event.payload.items:
        if item.line_total != (item.unit_price * item.quantity).quantize(Decimal("0.01")):
            raise PermanentEventError("payload contains an invalid line total")
    return event


def apply_event(session: Session, event: OrderCreatedEvent) -> bool:
    """Apply one valid event atomically. Returns False for a duplicate event ID."""
    with session.begin():
        existing = session.get(OrderProjection, event.aggregate_id)
        payload = event.payload.model_dump(mode="json")
        if existing is not None and existing.payload != payload:
            raise PermanentEventError("aggregate already has a projection from a conflicting event")
        values = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "aggregate_id": event.aggregate_id,
        }
        dialect = session.get_bind().dialect.name
        if dialect == "postgresql":
            claim = postgresql_insert(ProcessedEvent).values(**values)
            claim = claim.on_conflict_do_nothing(
                index_elements=[ProcessedEvent.event_id]
            ).returning(ProcessedEvent.event_id)
        elif dialect == "sqlite":
            claim = sqlite_insert(ProcessedEvent).values(**values)
            claim = claim.on_conflict_do_nothing(
                index_elements=[ProcessedEvent.event_id]
            ).returning(ProcessedEvent.event_id)
        else:
            if session.get(ProcessedEvent, event.event_id) is not None:
                return False
            session.add(ProcessedEvent(**values))
            claim = None
        if claim is not None:
            claimed_event_id = session.execute(claim).scalar_one_or_none()
            if claimed_event_id is None:
                session.execute(
                    update(ProcessedEvent)
                    .where(ProcessedEvent.event_id == event.event_id)
                    .values(
                        delivery_count=ProcessedEvent.delivery_count + 1,
                        last_seen_at=datetime.now(UTC),
                    )
                )
                return False
        if existing is None:
            session.add(
                OrderProjection(
                    order_id=event.payload.order_id,
                    source_event_id=event.event_id,
                    customer_id=event.payload.customer_id,
                    currency=event.payload.currency,
                    total=event.payload.total,
                    item_count=sum(item.quantity for item in event.payload.items),
                    payload=payload,
                    created_at=event.payload.created_at,
                )
            )
        session.flush()
    return True


def quarantine(session: Session, record: EventRecord, error_type: str, error_detail: str) -> bool:
    """Durably quarantine a broker record. Returns False if already quarantined."""
    with session.begin():
        existing = session.scalar(
            select(QuarantinedEvent.id).where(
                QuarantinedEvent.topic == record.topic,
                QuarantinedEvent.partition == record.partition,
                QuarantinedEvent.offset == record.offset,
            )
        )
        if existing is not None:
            return False
        session.add(
            QuarantinedEvent(
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                message_key=safe_text(record.key),
                raw_payload=safe_text(record.value),
                error_type=error_type[:100],
                error_detail=error_detail[:8000],
            )
        )
        session.flush()
    return True


def process_record(session_factory: sessionmaker[Session], record: EventRecord) -> str:
    try:
        event = parse_event(record.value)
    except PermanentEventError as exc:
        with session_factory() as session:
            created = quarantine(session, record, type(exc).__name__, str(exc))
        counters.increment("events_quarantined" if created else "events_duplicate_quarantine")
        logger.warning(
            "event_quarantined",
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            error=str(exc),
        )
        return "quarantined"

    token = event_id_var.set(str(event.event_id))
    try:
        try:
            with session_factory() as session:
                applied = apply_event(session, event)
        except PermanentEventError as exc:
            with session_factory() as session:
                quarantine(session, record, type(exc).__name__, str(exc))
            counters.increment("events_quarantined")
            logger.warning("event_quarantined", error=str(exc))
            return "quarantined"
        outcome = "processed" if applied else "duplicate"
        counters.increment(f"events_{outcome}")
        logger.info("event_processing_completed", outcome=outcome)
        return outcome
    finally:
        event_id_var.reset(token)


def _record_from_message(message) -> EventRecord:
    return EventRecord(
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
        key=message.key(),
        value=message.value(),
    )


def run(settings: Settings, stop: Event | None = None) -> None:
    stop = stop or Event()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "client.id": "fluxguard-order-consumer",
        }
    )
    consumer.subscribe([settings.kafka_orders_topic])
    logger.info("consumer_started", topic=settings.kafka_orders_topic)
    try:
        while not stop.is_set():
            message = consumer.poll(settings.kafka_poll_timeout_seconds)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                if message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.warning(
                        "consumer_topic_not_ready",
                        topic=settings.kafka_orders_topic,
                    )
                    stop.wait(2.0)
                    continue
                raise RuntimeError(str(message.error()))
            record = _record_from_message(message)
            while not stop.is_set():
                try:
                    process_record(SessionLocal, record)
                    consumer.commit(message=message, asynchronous=False)
                    break
                except Exception:
                    counters.increment("event_processing_failed")
                    logger.exception(
                        "event_processing_retrying",
                        topic=record.topic,
                        partition=record.partition,
                        offset=record.offset,
                    )
                    stop.wait(2.0)
    finally:
        consumer.close()


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
            counters.increment("consumer_worker_failed")
            logger.exception("consumer_worker_cycle_failed")
            time.sleep(2.0)


if __name__ == "__main__":
    main()
