import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from fluxguard.apps.api.main import create_app
from fluxguard.apps.worker.consumer import (
    ControlledTransientError,
    EventRecord,
    process_record,
    register_processing_failure,
)
from fluxguard.packages.common.config import Settings
from fluxguard.packages.common.db import get_session
from fluxguard.packages.common.incidents import Detection, analyze_impact, record_incident
from fluxguard.packages.common.models import (
    Incident,
    IncidentEvidence,
    IncidentSeverity,
    OrderProjection,
    QuarantinedEvent,
)


def event_data(*, version: int = 1, total: str = "10.00", fault: dict | None = None) -> dict:
    order_id = uuid4()
    data = {
        "event_id": str(uuid4()),
        "event_type": "orders.created.v1",
        "schema_version": version,
        "aggregate_id": str(order_id),
        "occurred_at": datetime.now(UTC).isoformat(),
        "payload": {
            "order_id": str(order_id),
            "customer_id": "private-customer",
            "items": [
                {
                    "product_id": "p1",
                    "quantity": 1,
                    "unit_price": "10.00",
                    "line_total": "10.00",
                }
            ],
            "currency": "USD",
            "total": total,
            "created_at": datetime.now(UTC).isoformat(),
        },
    }
    if fault:
        data["fault_injection"] = fault
    return data


def record(data: dict | bytes, offset: int) -> EventRecord:
    raw = data if isinstance(data, bytes) else json.dumps(data).encode()
    return EventRecord("orders.created.v1", 0, offset, b"key", raw)


@pytest.mark.parametrize(
    ("data", "expected_type", "expected_code"),
    [
        (event_data(version=99), "UNSUPPORTED_SCHEMA_VERSION", "UNSUPPORTED_SCHEMA_VERSION"),
        (b"not-json", "MALFORMED_EVENT", "MALFORMED_JSON"),
        (
            event_data(total="11.00"),
            "BUSINESS_INVARIANT_VIOLATION",
            "ORDER_TOTAL_MISMATCH",
        ),
    ],
)
def test_permanent_failures_create_incident_evidence_and_quarantine(
    session_factory, data, expected_type, expected_code
) -> None:
    assert process_record(session_factory, record(data, 20)) == "quarantined"
    with session_factory() as session:
        incident = session.scalar(select(Incident))
        evidence = session.scalar(select(IncidentEvidence))
        assert incident.incident_type == expected_type
        assert incident.error_code == expected_code
        assert evidence.kafka_offset == 20
        assert session.scalar(select(func.count()).select_from(QuarantinedEvent)) == 1


def test_poison_is_quarantined_and_following_healthy_event_processes(session_factory) -> None:
    settings = Settings(fault_injection_enabled=True)
    poison = event_data(fault={"scenario": "poison_message", "scenario_id": str(uuid4())})
    assert process_record(session_factory, record(poison, 30), settings) == "quarantined"
    assert process_record(session_factory, record(event_data(), 31), settings) == "processed"
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(QuarantinedEvent)) == 1
        assert session.scalar(select(func.count()).select_from(OrderProjection)) == 1
        assert session.scalar(select(Incident.incident_type)) == "POISON_MESSAGE"


def test_repeated_transient_failure_creates_incident_then_recovers(session_factory) -> None:
    settings = Settings(
        fault_injection_enabled=True,
        consumer_retry_incident_threshold=3,
    )
    item = record(
        event_data(
            fault={
                "scenario": "transient_processing_failure",
                "scenario_id": str(uuid4()),
                "failures": 3,
            }
        ),
        40,
    )
    for _ in range(3):
        with pytest.raises(ControlledTransientError) as captured:
            process_record(session_factory, item, settings)
        register_processing_failure(session_factory, item, captured.value, 3)
    assert process_record(session_factory, item, settings) == "processed"
    with session_factory() as session:
        incident = session.scalar(select(Incident))
        assert incident.incident_type == "REPEATED_TRANSIENT_FAILURE"
        assert incident.occurrence_count == 1
        assert session.scalar(select(func.count()).select_from(OrderProjection)) == 1


def test_correlation_preserves_evidence_and_redacts_sensitive_fields(session_factory) -> None:
    scenario_id = str(uuid4())
    detection = Detection(
        incident_type="POISON_MESSAGE",
        severity=IncidentSeverity.HIGH,
        error_code="UNPROCESSABLE_POISON_MESSAGE",
        summary="controlled poison",
        source_component="order_consumer",
        fingerprint_scope={"scenario_id": scenario_id},
        evidence_type="QUARANTINED_EVENT",
        evidence_payload={
            "scenario_id": scenario_id,
            "password": "must-not-survive",
            "customer_id": "private-customer",
        },
        topic="orders.created.v1",
        partition=0,
        offset=50,
    )
    with session_factory() as session, session.begin():
        incident, _ = record_incident(session, detection)
    second = Detection(**{**detection.__dict__, "offset": 51})
    with session_factory() as session, session.begin():
        record_incident(session, second)
    with session_factory() as session:
        stored = session.get(Incident, incident.incident_id)
        payloads = session.scalars(
            select(IncidentEvidence.payload).where(
                IncidentEvidence.incident_id == incident.incident_id
            )
        ).all()
        assert stored.occurrence_count == 2
        assert len(payloads) == 2
        assert all(payload["password"] == "[REDACTED]" for payload in payloads)
        assert all(payload["customer_id"] == "[REDACTED]" for payload in payloads)


def test_same_evidence_is_idempotent(session_factory) -> None:
    detection = Detection(
        incident_type="MALFORMED_EVENT",
        severity=IncidentSeverity.HIGH,
        error_code="MALFORMED_JSON",
        summary="bad json",
        source_component="order_consumer",
        fingerprint_scope={"raw_sha256": "abc"},
        evidence_type="QUARANTINED_EVENT",
        evidence_payload={"raw_sha256": "abc"},
        topic="orders.created.v1",
        partition=0,
        offset=60,
    )
    for occurrence in (
        detection,
        Detection(
            **{
                **detection.__dict__,
                "evidence_payload": {
                    "raw_sha256": "abc",
                    "worker_observation": "different enrichment",
                },
            }
        ),
    ):
        with session_factory() as session, session.begin():
            record_incident(session, occurrence)
    with session_factory() as session:
        assert session.scalar(select(Incident.occurrence_count)) == 1
        assert session.scalar(select(func.count()).select_from(IncidentEvidence)) == 1


def test_impact_uses_verified_evidence_and_marks_unknowns(session_factory) -> None:
    data = event_data(total="11.00")
    assert process_record(session_factory, record(data, 70)) == "quarantined"
    with session_factory() as session:
        incident = session.scalar(select(Incident))
        impact = analyze_impact(session, incident)
        assert impact.kafka_positions == [
            {"topic": "orders.created.v1", "partition": 0, "offset": 70}
        ]
        assert impact.quarantine["matching_records"] == 1
        assert impact.downstream_projection["persisted_projection_ids"] == []
        assert any("record loss" in value for value in impact.unavailable)


def test_incident_api_is_disabled_unless_development_flag_is_explicit(
    session_factory, monkeypatch
) -> None:
    def sessions():
        with session_factory() as session:
            yield session

    app = create_app(sessions)
    app.dependency_overrides[get_session] = sessions
    client = TestClient(app)
    assert client.get("/api/v1/incidents").status_code == 404

    monkeypatch.setattr(
        "fluxguard.apps.api.incidents.get_settings",
        lambda: Settings(environment="development", incident_api_enabled=True),
    )
    with session_factory() as session, session.begin():
        incident, _ = record_incident(
            session,
            Detection(
                incident_type="MALFORMED_EVENT",
                severity=IncidentSeverity.HIGH,
                error_code="MALFORMED_JSON",
                summary="internal parser detail",
                source_component="order_consumer",
                fingerprint_scope={"raw_sha256": "api-test"},
                evidence_type="QUARANTINED_EVENT",
                evidence_payload={
                    "raw_sha256": "api-test",
                    "error_detail": "private parser internals",
                },
                topic="orders.created.v1",
                partition=0,
                offset=80,
            ),
        )
    response = client.get("/api/v1/incidents?limit=10&offset=0")
    assert response.status_code == 200
    assert response.json()["total"] == 1
    assert response.json()["data"][0]["summary"] == "event payload is not valid JSON"
    evidence = client.get(f"/api/v1/incidents/{incident.incident_id}/evidence")
    assert evidence.status_code == 200
    assert "error_detail" not in evidence.json()["data"][0]["payload"]
    impact = client.get(f"/api/v1/incidents/{incident.incident_id}/impact")
    assert impact.status_code == 200
    assert impact.json()["failed_stage"] == "order_consumer"
