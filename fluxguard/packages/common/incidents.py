import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from fluxguard.packages.common.metrics import counters
from fluxguard.packages.common.models import (
    Incident,
    IncidentEvidence,
    IncidentSeverity,
    IncidentStatus,
    OrderProjection,
    QuarantinedEvent,
)
from fluxguard.packages.common.time import utc_now
from fluxguard.packages.contracts.incidents import EvidenceView, ImpactView, IncidentView

MAX_EVIDENCE_JSON_BYTES = 32_768
MAX_STRING_LENGTH = 1_000
MAX_COLLECTION_ITEMS = 50
SENSITIVE_KEY = re.compile(
    r"password|passwd|secret|token|authorization|api[_-]?key|customer_id|email|phone|name",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Detection:
    incident_type: str
    severity: IncidentSeverity
    error_code: str
    summary: str
    source_component: str
    fingerprint_scope: dict[str, Any]
    evidence_type: str
    evidence_payload: dict[str, Any] = field(default_factory=dict)
    event_id: UUID | None = None
    topic: str | None = None
    partition: int | None = None
    offset: int | None = None
    observed_at: datetime | None = None


def redact(value: Any, *, depth: int = 0) -> Any:
    if depth >= 8:
        return "[MAX_DEPTH]"
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= MAX_COLLECTION_ITEMS:
                result["_truncated"] = True
                break
            result[str(key)] = (
                "[REDACTED]" if SENSITIVE_KEY.search(str(key)) else redact(item, depth=depth + 1)
            )
        return result
    if isinstance(value, list | tuple):
        items = [redact(item, depth=depth + 1) for item in value[:MAX_COLLECTION_ITEMS]]
        if len(value) > MAX_COLLECTION_ITEMS:
            items.append("[TRUNCATED]")
        return items
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value[:MAX_STRING_LENGTH]
    if value is None or isinstance(value, bool | int | float):
        return value
    return str(value)[:MAX_STRING_LENGTH]


def bounded_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sanitized = redact(payload)
    encoded = json.dumps(sanitized, sort_keys=True, default=str).encode()
    if len(encoded) <= MAX_EVIDENCE_JSON_BYTES:
        return sanitized
    return {
        "truncated": True,
        "original_sha256": hashlib.sha256(encoded).hexdigest(),
        "original_size_bytes": len(encoded),
    }


def incident_fingerprint(detection: Detection) -> str:
    material = {
        "incident_type": detection.incident_type,
        "source_component": detection.source_component,
        "error_code": detection.error_code,
        "scope": redact(detection.fingerprint_scope),
    }
    canonical = json.dumps(material, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _insert_for(session: Session, model, values: dict[str, Any], conflict_column):
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        statement = postgresql_insert(model).values(**values)
    elif dialect == "sqlite":
        statement = sqlite_insert(model).values(**values)
    else:
        return None
    return statement.on_conflict_do_nothing(index_elements=[conflict_column])


def record_incident(session: Session, detection: Detection) -> tuple[Incident, bool]:
    """Correlate an occurrence and retain deduplicated evidence in the caller transaction."""
    now = detection.observed_at or utc_now()
    fingerprint = incident_fingerprint(detection)
    incident_id = uuid4()
    incident_values = {
        "incident_id": incident_id,
        "incident_type": detection.incident_type,
        "severity": detection.severity.value,
        "status": IncidentStatus.OPEN.value,
        "source_component": detection.source_component,
        "detected_at": now,
        "updated_at": now,
        "first_seen_at": now,
        "last_seen_at": now,
        "occurrence_count": 1,
        "error_code": detection.error_code,
        "summary": detection.summary[:500],
        "fingerprint": fingerprint,
        "details": bounded_payload(detection.fingerprint_scope),
    }
    statement = _insert_for(session, Incident, incident_values, Incident.fingerprint)
    created = False
    if statement is not None:
        inserted = session.execute(statement.returning(Incident.incident_id)).scalar_one_or_none()
        created = inserted is not None
    else:
        existing = session.scalar(select(Incident).where(Incident.fingerprint == fingerprint))
        if existing is None:
            session.add(Incident(**incident_values))
            session.flush()
            created = True

    incident = session.scalar(
        select(Incident).where(Incident.fingerprint == fingerprint).with_for_update()
    )
    if incident is None:
        raise RuntimeError("incident correlation insert did not produce a row")

    safe_payload = bounded_payload(detection.evidence_payload)
    evidence_material = {
        "fingerprint": fingerprint,
        "evidence_type": detection.evidence_type,
        "source": detection.source_component,
        "event_id": detection.event_id,
        "topic": detection.topic,
        "partition": detection.partition,
        "offset": detection.offset,
    }
    # A Kafka coordinate is the durable identity of a delivered record. Different
    # workers may enrich its evidence payload differently, but replaying that same
    # coordinate must never create another business occurrence.
    if detection.topic is None or detection.partition is None or detection.offset is None:
        evidence_material["payload"] = safe_payload
    dedup_key = hashlib.sha256(
        json.dumps(evidence_material, sort_keys=True, default=str).encode()
    ).hexdigest()
    evidence_values = {
        "evidence_id": uuid4(),
        "incident_id": incident.incident_id,
        "evidence_type": detection.evidence_type,
        "source": detection.source_component,
        "event_id": detection.event_id,
        "kafka_topic": detection.topic,
        "kafka_partition": detection.partition,
        "kafka_offset": detection.offset,
        "observed_at": now,
        "payload": safe_payload,
        "dedup_key": dedup_key,
    }
    evidence_statement = _insert_for(
        session, IncidentEvidence, evidence_values, IncidentEvidence.dedup_key
    )
    evidence_created = False
    if evidence_statement is not None:
        inserted_evidence = session.execute(
            evidence_statement.returning(IncidentEvidence.evidence_id)
        ).scalar_one_or_none()
        evidence_created = inserted_evidence is not None
    elif (
        session.scalar(
            select(IncidentEvidence.evidence_id).where(IncidentEvidence.dedup_key == dedup_key)
        )
        is None
    ):
        session.add(IncidentEvidence(**evidence_values))
        evidence_created = True

    if evidence_created and not created:
        incident.occurrence_count += 1
        incident.last_seen_at = now
        incident.updated_at = now
    session.flush()
    if evidence_created:
        counters.increment(f"incidents_{detection.incident_type.lower()}")
    return incident, evidence_created


def incident_view(incident: Incident) -> IncidentView:
    public_summary = (
        "event payload is not valid JSON"
        if incident.error_code == "MALFORMED_JSON"
        else incident.summary
    )
    return IncidentView(
        incident_id=incident.incident_id,
        incident_type=incident.incident_type,
        severity=IncidentSeverity(incident.severity),
        status=IncidentStatus(incident.status),
        source_component=incident.source_component,
        detected_at=incident.detected_at,
        updated_at=incident.updated_at,
        first_seen_at=incident.first_seen_at,
        last_seen_at=incident.last_seen_at,
        occurrence_count=incident.occurrence_count,
        error_code=incident.error_code,
        summary=public_summary,
        fingerprint=incident.fingerprint,
        metadata=incident.details,
    )


def evidence_view(evidence: IncidentEvidence) -> EvidenceView:
    public_payload = {
        key: value for key, value in evidence.payload.items() if key != "error_detail"
    }
    return EvidenceView(
        evidence_id=evidence.evidence_id,
        incident_id=evidence.incident_id,
        evidence_type=evidence.evidence_type,
        source=evidence.source,
        event_id=evidence.event_id,
        kafka_topic=evidence.kafka_topic,
        kafka_partition=evidence.kafka_partition,
        kafka_offset=evidence.kafka_offset,
        observed_at=evidence.observed_at,
        payload=public_payload,
    )


def analyze_impact(session: Session, incident: Incident) -> ImpactView:
    evidence = session.scalars(
        select(IncidentEvidence)
        .where(IncidentEvidence.incident_id == incident.incident_id)
        .order_by(IncidentEvidence.observed_at)
    ).all()
    event_ids = sorted({item.event_id for item in evidence if item.event_id}, key=str)
    positions = [
        {"topic": item.kafka_topic, "partition": item.kafka_partition, "offset": item.kafka_offset}
        for item in evidence
        if item.kafka_topic is not None
    ]
    quarantine_matches = 0
    for position in positions:
        quarantine_matches += session.scalar(
            select(func.count())
            .select_from(QuarantinedEvent)
            .where(
                QuarantinedEvent.topic == position["topic"],
                QuarantinedEvent.partition == position["partition"],
                QuarantinedEvent.offset == position["offset"],
            )
        )
    aggregate_ids = {
        item.payload.get("aggregate_id")
        for item in evidence
        if isinstance(item.payload, dict) and item.payload.get("aggregate_id")
    }
    projection_ids: list[str] = []
    for aggregate_id in aggregate_ids:
        try:
            parsed = UUID(str(aggregate_id))
        except ValueError:
            continue
        if session.get(OrderProjection, parsed) is not None:
            projection_ids.append(str(parsed))
    schema_versions = sorted(
        {
            int(item.payload["schema_version"])
            for item in evidence
            if isinstance(item.payload, dict)
            and isinstance(item.payload.get("schema_version"), int)
        }
    )
    unavailable = []
    if not positions:
        unavailable.append("Kafka position was not available at detection time")
    unavailable.append("Exact record loss is not inferred from lag or incident evidence")
    return ImpactView(
        incident_id=incident.incident_id,
        pipeline=["API", "Outbox", "Redpanda", "Consumer", "Projection"],
        failed_stage=incident.source_component,
        affected_event_ids=event_ids,
        kafka_positions=positions,
        quarantine={"verified": bool(positions), "matching_records": quarantine_matches},
        downstream_projection={
            "known_aggregate_ids": sorted(str(value) for value in aggregate_ids),
            "persisted_projection_ids": sorted(projection_ids),
        },
        event_count=len(evidence),
        earliest_occurrence=incident.first_seen_at,
        latest_occurrence=incident.last_seen_at,
        schema_versions=schema_versions,
        evidence_ids=[item.evidence_id for item in evidence],
        unavailable=unavailable,
    )
