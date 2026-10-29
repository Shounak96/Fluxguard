import base64
import hashlib
import json
import signal
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from threading import Event
from typing import Protocol
from uuid import UUID

from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError
from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, sessionmaker

from fluxguard.packages.common.config import Settings, get_settings
from fluxguard.packages.common.db import SessionLocal
from fluxguard.packages.common.incidents import Detection, record_incident
from fluxguard.packages.common.logging import (
    configure_logging,
    event_id_var,
    get_logger,
)
from fluxguard.packages.common.metrics import counters
from fluxguard.packages.common.models import (
    ConsumerRetryState,
    IncidentSeverity,
    OrderProjection,
    ProcessedEvent,
    QuarantinedEvent,
)
from fluxguard.packages.contracts.events import OrderCreatedEvent

logger = get_logger(__name__)
MAX_QUARANTINE_PAYLOAD_BYTES = 1_000_000


class PermanentEventError(Exception):
    def __init__(
        self,
        message: str,
        *,
        incident_type: str = "MALFORMED_EVENT",
        severity: IncidentSeverity = IncidentSeverity.HIGH,
        error_code: str = "MALFORMED_EVENT_ENVELOPE",
        context: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.incident_type = incident_type
        self.severity = severity
        self.error_code = error_code
        self.context = context or {}


class ControlledTransientError(RuntimeError):
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


def parse_event(raw: bytes | None, *, fault_enabled: bool = False) -> OrderCreatedEvent:
    if raw is None:
        raise PermanentEventError("event payload is null", error_code="NULL_EVENT_PAYLOAD")
    try:
        decoded = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PermanentEventError(
            "event payload is not valid JSON",
            error_code="MALFORMED_JSON",
            context={"raw_sha256": hashlib.sha256(raw).hexdigest(), "raw_size": len(raw)},
        ) from exc
    if not isinstance(decoded, dict):
        raise PermanentEventError(
            "event envelope must be a JSON object", error_code="MALFORMED_EVENT_ENVELOPE"
        )
    fault = decoded.pop("fault_injection", None)
    if fault_enabled and isinstance(fault, dict) and fault.get("scenario") == "poison_message":
        raise PermanentEventError(
            "controlled poison message",
            incident_type="POISON_MESSAGE",
            severity=IncidentSeverity.HIGH,
            error_code="UNPROCESSABLE_POISON_MESSAGE",
            context={"scenario_id": fault.get("scenario_id")},
        )
    if decoded.get("schema_version") != 1:
        raise PermanentEventError(
            "unsupported event schema_version",
            incident_type="UNSUPPORTED_SCHEMA_VERSION",
            severity=IncidentSeverity.MEDIUM,
            error_code="UNSUPPORTED_SCHEMA_VERSION",
            context={"schema_version": decoded.get("schema_version")},
        )
    try:
        event = OrderCreatedEvent.model_validate(decoded)
    except ValidationError as exc:
        raise PermanentEventError(
            "event envelope failed contract validation",
            error_code="MALFORMED_EVENT_ENVELOPE",
        ) from exc
    if event.aggregate_id != event.payload.order_id:
        raise PermanentEventError(
            "aggregate_id does not match payload.order_id",
            incident_type="BUSINESS_INVARIANT_VIOLATION",
            error_code="AGGREGATE_ID_MISMATCH",
            context={"aggregate_id": str(event.aggregate_id)},
        )
    calculated = sum((item.line_total for item in event.payload.items), start=Decimal("0.00"))
    if calculated != event.payload.total:
        raise PermanentEventError(
            "payload total does not equal its line totals",
            incident_type="BUSINESS_INVARIANT_VIOLATION",
            error_code="ORDER_TOTAL_MISMATCH",
            context={
                "aggregate_id": str(event.aggregate_id),
                "schema_version": event.schema_version,
            },
        )
    for item in event.payload.items:
        if item.line_total != (item.unit_price * item.quantity).quantize(Decimal("0.01")):
            raise PermanentEventError(
                "payload contains an invalid line total",
                incident_type="BUSINESS_INVARIANT_VIOLATION",
                error_code="LINE_TOTAL_MISMATCH",
                context={"aggregate_id": str(event.aggregate_id)},
            )
    return event


def apply_event_in_transaction(session: Session, event: OrderCreatedEvent) -> bool:
    """Apply a valid event inside the caller-owned transaction."""
    existing = session.get(OrderProjection, event.aggregate_id)
    payload = event.payload.model_dump(mode="json")
    if existing is not None and existing.payload != payload:
        raise PermanentEventError(
            "aggregate already has a projection from a conflicting event",
            incident_type="BUSINESS_INVARIANT_VIOLATION",
            error_code="CONFLICTING_AGGREGATE_EVENT",
            context={"aggregate_id": str(event.aggregate_id)},
        )
    values = {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "aggregate_id": event.aggregate_id,
    }
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        claim = postgresql_insert(ProcessedEvent).values(**values)
        claim = claim.on_conflict_do_nothing(index_elements=[ProcessedEvent.event_id]).returning(
            ProcessedEvent.event_id
        )
    elif dialect == "sqlite":
        claim = sqlite_insert(ProcessedEvent).values(**values)
        claim = claim.on_conflict_do_nothing(index_elements=[ProcessedEvent.event_id]).returning(
            ProcessedEvent.event_id
        )
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


def apply_event(session: Session, event: OrderCreatedEvent) -> bool:
    """Apply one valid event atomically. Returns False for a duplicate event ID."""
    with session.begin():
        return apply_event_in_transaction(session, event)


def _quarantine_in_transaction(
    session: Session, record: EventRecord, error_type: str, error_detail: str
) -> bool:
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


def quarantine(session: Session, record: EventRecord, error_type: str, error_detail: str) -> bool:
    """Durably quarantine a broker record. Returns False if already quarantined."""
    with session.begin():
        return _quarantine_in_transaction(session, record, error_type, error_detail)


def _decoded_identity(raw: bytes | None) -> tuple[UUID | None, dict]:
    if raw is None:
        return None, {}
    try:
        decoded = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, {}
    if not isinstance(decoded, dict):
        return None, {}
    try:
        event_id = UUID(str(decoded.get("event_id")))
    except (TypeError, ValueError):
        event_id = None
    return event_id, decoded


def quarantine_with_incident(
    session_factory: sessionmaker[Session], record: EventRecord, error: PermanentEventError
) -> tuple[bool, UUID]:
    event_id, decoded = _decoded_identity(record.value)
    raw_hash = hashlib.sha256(record.value or b"").hexdigest()
    scenario = decoded.get("fault_injection") if isinstance(decoded, dict) else None
    scenario_id = scenario.get("scenario_id") if isinstance(scenario, dict) else None
    scope = {
        "topic": record.topic,
        "error_code": error.error_code,
        "scenario_id": scenario_id,
        "raw_sha256": raw_hash if error.incident_type == "MALFORMED_EVENT" else None,
        **error.context,
    }
    evidence_payload = {
        "error_code": error.error_code,
        "error_detail": str(error)[:1000],
        "raw_sha256": raw_hash,
        "raw_size": len(record.value or b""),
        "schema_version": decoded.get("schema_version"),
        "aggregate_id": decoded.get("aggregate_id"),
        "scenario_id": scenario_id,
    }
    with session_factory() as session, session.begin():
        created = _quarantine_in_transaction(session, record, error.error_code, str(error))
        incident, _ = record_incident(
            session,
            Detection(
                incident_type=error.incident_type,
                severity=error.severity,
                error_code=error.error_code,
                summary=str(error),
                source_component="order_consumer",
                fingerprint_scope=scope,
                evidence_type="QUARANTINED_EVENT",
                evidence_payload=evidence_payload,
                event_id=event_id,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
            ),
        )
    return created, incident.incident_id


def _controlled_transient_pending(
    session_factory: sessionmaker[Session], record: EventRecord, settings: Settings
) -> bool:
    if not settings.fault_injection_enabled:
        return False
    _, decoded = _decoded_identity(record.value)
    fault = decoded.get("fault_injection") if isinstance(decoded, dict) else None
    if not isinstance(fault, dict) or fault.get("scenario") != "transient_processing_failure":
        return False
    failures = max(1, min(int(fault.get("failures", 3)), 20))
    with session_factory() as session:
        attempts = session.scalar(
            select(ConsumerRetryState.attempt_count).where(
                ConsumerRetryState.topic == record.topic,
                ConsumerRetryState.partition == record.partition,
                ConsumerRetryState.offset == record.offset,
            )
        )
    return (attempts or 0) < failures


def register_processing_failure(
    session_factory: sessionmaker[Session],
    record: EventRecord,
    error: Exception,
    threshold: int,
) -> int:
    event_id, decoded = _decoded_identity(record.value)
    now = datetime.now(UTC)
    with session_factory() as session, session.begin():
        state = session.scalar(
            select(ConsumerRetryState)
            .where(
                ConsumerRetryState.topic == record.topic,
                ConsumerRetryState.partition == record.partition,
                ConsumerRetryState.offset == record.offset,
            )
            .with_for_update()
        )
        if state is None:
            state = ConsumerRetryState(
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                event_id=event_id,
                attempt_count=1,
                first_attempt_at=now,
                last_attempt_at=now,
                last_error=str(error)[:1000],
            )
            session.add(state)
            session.flush()
        else:
            state.attempt_count += 1
            state.last_attempt_at = now
            state.last_error = str(error)[:1000]
        if state.attempt_count >= threshold:
            scenario = decoded.get("fault_injection") if isinstance(decoded, dict) else None
            scenario_id = scenario.get("scenario_id") if isinstance(scenario, dict) else None
            incident, _ = record_incident(
                session,
                Detection(
                    incident_type="REPEATED_TRANSIENT_FAILURE",
                    severity=IncidentSeverity.MEDIUM,
                    error_code="CONSUMER_RETRY_THRESHOLD_EXCEEDED",
                    summary="consumer processing repeatedly failed and remains retryable",
                    source_component="order_consumer",
                    fingerprint_scope={
                        "topic": record.topic,
                        "partition": record.partition,
                        "offset": record.offset,
                        "scenario_id": scenario_id,
                    },
                    evidence_type="PROCESSING_RETRY",
                    evidence_payload={
                        "attempt": state.attempt_count,
                        "error": str(error)[:1000],
                        "scenario_id": scenario_id,
                    },
                    event_id=event_id,
                    topic=record.topic,
                    partition=record.partition,
                    offset=record.offset,
                ),
            )
            logger.warning(
                "processing_retry_incident_recorded",
                incident_id=str(incident.incident_id),
                source_component="order_consumer",
                error_code="CONSUMER_RETRY_THRESHOLD_EXCEEDED",
                kafka_topic=record.topic,
                kafka_partition=record.partition,
                kafka_offset=record.offset,
            )
        return state.attempt_count


def clear_retry_state(session_factory: sessionmaker[Session], record: EventRecord) -> None:
    with session_factory() as session, session.begin():
        session.execute(
            delete(ConsumerRetryState).where(
                ConsumerRetryState.topic == record.topic,
                ConsumerRetryState.partition == record.partition,
                ConsumerRetryState.offset == record.offset,
            )
        )


def process_record(
    session_factory: sessionmaker[Session],
    record: EventRecord,
    settings: Settings | None = None,
) -> str:
    settings = settings or get_settings()
    if _controlled_transient_pending(session_factory, record, settings):
        raise ControlledTransientError("controlled transient processing failure")
    try:
        event = parse_event(record.value, fault_enabled=settings.fault_injection_enabled)
    except PermanentEventError as exc:
        created, incident_id = quarantine_with_incident(session_factory, record, exc)
        counters.increment("events_quarantined" if created else "events_duplicate_quarantine")
        logger.warning(
            "event_quarantined",
            incident_id=str(incident_id),
            source_component="order_consumer",
            error_code=exc.error_code,
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
            _, incident_id = quarantine_with_incident(session_factory, record, exc)
            counters.increment("events_quarantined")
            logger.warning(
                "event_quarantined",
                incident_id=str(incident_id),
                source_component="order_consumer",
                error_code=exc.error_code,
                error=str(exc),
            )
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
                    process_record(SessionLocal, record, settings)
                    clear_retry_state(SessionLocal, record)
                    consumer.commit(message=message, asynchronous=False)
                    break
                except Exception as exc:
                    counters.increment("event_processing_failed")
                    counters.increment("consumer_retries")
                    try:
                        attempt = register_processing_failure(
                            SessionLocal,
                            record,
                            exc,
                            settings.consumer_retry_incident_threshold,
                        )
                    except Exception:
                        attempt = None
                        logger.exception(
                            "processing_retry_evidence_failed",
                            source_component="order_consumer",
                            kafka_topic=record.topic,
                            kafka_partition=record.partition,
                            kafka_offset=record.offset,
                        )
                    logger.exception(
                        "event_processing_retrying",
                        source_component="order_consumer",
                        error_code="TRANSIENT_PROCESSING_FAILURE",
                        kafka_topic=record.topic,
                        kafka_partition=record.partition,
                        kafka_offset=record.offset,
                        attempt=attempt,
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
