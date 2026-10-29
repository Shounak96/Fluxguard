import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, delete, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from fluxguard.packages.common.incidents import Detection, record_incident
from fluxguard.packages.common.models import (
    IncidentEvidence,
    IncidentSeverity,
    RecoveryLifecycleEvent,
    RecoveryPlanEntity,
    RecoveryVerification,
)
from fluxguard.packages.common.recoveries import (
    InvalidRecoveryTransitionError,
    RecoveryEvidenceOwnershipError,
    RecoveryPlanConflictError,
    create_recovery_plan,
    get_recovery_plan_history,
    record_recovery_verification,
)
from fluxguard.packages.common.recovery_policy import (
    RECOVERY_POLICY_VERSION,
    build_recovery_plan,
    evaluate_recovery_plan,
)
from fluxguard.packages.contracts.recovery import (
    AuthoritativeEventData,
    AuthoritativeEventSource,
    EligibilityStatus,
    KafkaCoordinate,
    RecoveryActionType,
    RecoveryEvidence,
    RecoveryLifecycleState,
    RecoveryPlanSource,
    RecoveryStateSnapshot,
    RecoveryTarget,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1",
        reason="set RUN_INTEGRATION=1 with migrated PostgreSQL",
    ),
]

EVENT_ID = UUID("10000000-0000-4000-8000-000000000001")
ORDER_ID = UUID("10000000-0000-4000-8000-000000000002")
COORDINATE = KafkaCoordinate(topic="orders.created.v1", partition=0, offset=900_001)


def _raw_event(event_id: UUID = EVENT_ID, order_id: UUID = ORDER_ID) -> bytes:
    payload = {
        "event_id": str(event_id),
        "event_type": "orders.created.v1",
        "schema_version": 1,
        "aggregate_id": str(order_id),
        "occurred_at": "2026-01-01T00:00:00Z",
        "payload": {
            "order_id": str(order_id),
            "customer_id": "recovery-persistence",
            "items": [
                {
                    "product_id": "product-1",
                    "quantity": 2,
                    "unit_price": "24.99",
                    "line_total": "49.98",
                }
            ],
            "currency": "USD",
            "total": "49.98",
            "created_at": "2026-01-01T00:00:00Z",
        },
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _factory():
    return sessionmaker(
        bind=create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True),
        expire_on_commit=False,
    )


def _create_incident(factory, *, event_id: UUID = EVENT_ID, offset: int = 900_001):
    raw = _raw_event(event_id=event_id)
    raw_hash = hashlib.sha256(raw).hexdigest()
    with factory() as session, session.begin():
        incident, _ = record_incident(
            session,
            Detection(
                incident_type="REPEATED_TRANSIENT_FAILURE",
                severity=IncidentSeverity.MEDIUM,
                error_code="CONSUMER_RETRY_THRESHOLD_EXCEEDED",
                summary="recovery persistence test incident",
                source_component="recovery_persistence_test",
                fingerprint_scope={"scenario_id": str(uuid4())},
                evidence_type="PROCESSING_RETRY",
                evidence_payload={"raw_sha256": raw_hash},
                event_id=event_id,
                topic="orders.created.v1",
                partition=0,
                offset=offset,
            ),
        )
        evidence = session.scalar(
            select(IncidentEvidence).where(IncidentEvidence.incident_id == incident.incident_id)
        )
        return incident.incident_id, evidence.evidence_id, raw, raw_hash


def _plan_and_state(incident_id, evidence_id, raw, raw_hash):
    evidence = RecoveryEvidence(
        evidence_id=evidence_id,
        incident_id=incident_id,
        event_id=EVENT_ID,
        kafka_coordinate=COORDINATE,
        content_sha256=raw_hash,
    )
    state = RecoveryStateSnapshot(
        incident_id=incident_id,
        evidence=(evidence,),
        authoritative_event=AuthoritativeEventData(
            source=AuthoritativeEventSource.BROKER,
            raw_event=raw,
            sha256=raw_hash,
            evidence_id=evidence_id,
            kafka_coordinate=COORDINATE,
        ),
    )
    plan = build_recovery_plan(
        action=RecoveryActionType.REPLAY_VERIFIED_EVENT,
        incident_id=incident_id,
        target=RecoveryTarget(
            event_id=EVENT_ID,
            aggregate_id=ORDER_ID,
            kafka_coordinate=COORDINATE,
        ),
        evidence_ids=[evidence_id],
        state=state,
        expected_event_sha256=raw_hash,
    )
    return plan, state


def test_plan_creation_is_idempotent_and_history_is_read_only() -> None:
    factory = _factory()
    plan, _ = _plan_and_state(*_create_incident(factory))
    with factory() as session, session.begin():
        first, created = create_recovery_plan(
            session, plan, source=RecoveryPlanSource.SYSTEM, created_by="integration-test"
        )
        second, replayed = create_recovery_plan(
            session, plan, source=RecoveryPlanSource.OPERATOR, created_by="different-actor"
        )
        history = get_recovery_plan_history(session, plan.plan_id)
        assert created is True
        assert replayed is False
        assert first == second
        assert first.current_state == RecoveryLifecycleState.PROPOSED
        assert len(history.lifecycle_events) == 1
        assert history.verifications == ()
        stored = session.get(RecoveryPlanEntity, plan.plan_id)
        assert "raw_event" not in json.dumps(stored.canonical_inputs)


def test_conflicting_duplicate_plan_is_rejected() -> None:
    factory = _factory()
    plan, _ = _plan_and_state(*_create_incident(factory))
    with factory() as session, session.begin():
        create_recovery_plan(
            session, plan, source=RecoveryPlanSource.SYSTEM, created_by="integration-test"
        )
    conflicting = plan.model_copy(update={"action": RecoveryActionType.RETAIN_QUARANTINE})
    with factory() as session, pytest.raises(RecoveryPlanConflictError), session.begin():
        create_recovery_plan(
            session,
            conflicting,
            source=RecoveryPlanSource.SYSTEM,
            created_by="integration-test",
        )


def test_foreign_incident_evidence_is_rejected() -> None:
    factory = _factory()
    incident_id, _, raw, raw_hash = _create_incident(factory)
    _, foreign_evidence_id, _, _ = _create_incident(factory, event_id=uuid4(), offset=900_002)
    foreign_evidence = RecoveryEvidence(
        evidence_id=foreign_evidence_id,
        incident_id=incident_id,
        event_id=EVENT_ID,
        kafka_coordinate=COORDINATE,
        content_sha256=raw_hash,
    )
    state = RecoveryStateSnapshot(incident_id=incident_id, evidence=(foreign_evidence,))
    plan = build_recovery_plan(
        action=RecoveryActionType.RETAIN_QUARANTINE,
        incident_id=incident_id,
        target=RecoveryTarget(event_id=EVENT_ID, kafka_coordinate=COORDINATE),
        evidence_ids=[foreign_evidence_id],
        state=state,
    )
    with factory() as session, pytest.raises(RecoveryEvidenceOwnershipError), session.begin():
        create_recovery_plan(
            session,
            plan,
            source=RecoveryPlanSource.SYSTEM,
            created_by="integration-test",
        )


def test_verification_transition_is_single_and_audited() -> None:
    factory = _factory()
    plan, state = _plan_and_state(*_create_incident(factory))
    decision = evaluate_recovery_plan(plan, state)
    assert decision.status == EligibilityStatus.ELIGIBLE
    with factory() as session, session.begin():
        create_recovery_plan(
            session, plan, source=RecoveryPlanSource.SYSTEM, created_by="integration-test"
        )
        record_recovery_verification(
            session,
            plan.plan_id,
            decision,
            verifier_version=RECOVERY_POLICY_VERSION,
        )
        with pytest.raises(InvalidRecoveryTransitionError):
            record_recovery_verification(
                session,
                plan.plan_id,
                decision,
                verifier_version=RECOVERY_POLICY_VERSION,
            )
        history = get_recovery_plan_history(session, plan.plan_id)
        assert history.plan.current_state == RecoveryLifecycleState.VERIFIED
        assert len(history.verifications) == 1
        assert history.verifications[0].state_digest == plan.expected_state_digest
        assert [item.to_state for item in history.lifecycle_events] == [
            RecoveryLifecycleState.PROPOSED,
            RecoveryLifecycleState.VERIFIED,
        ]


def test_audit_rows_reject_update_and_delete() -> None:
    factory = _factory()
    plan, state = _plan_and_state(*_create_incident(factory))
    decision = evaluate_recovery_plan(plan, state)
    with factory() as session, session.begin():
        create_recovery_plan(
            session, plan, source=RecoveryPlanSource.SYSTEM, created_by="integration-test"
        )
        verification = record_recovery_verification(
            session,
            plan.plan_id,
            decision,
            verifier_version=RECOVERY_POLICY_VERSION,
        )
    with factory() as session, pytest.raises(DBAPIError), session.begin():
        session.execute(
            update(RecoveryLifecycleEvent)
            .where(RecoveryLifecycleEvent.plan_id == plan.plan_id)
            .values(reason_code="SILENT_EDIT")
        )
    with factory() as session, pytest.raises(DBAPIError), session.begin():
        session.execute(
            delete(RecoveryVerification).where(
                RecoveryVerification.verification_id == verification.verification_id
            )
        )


def test_plan_creation_rolls_back_with_caller_transaction() -> None:
    factory = _factory()
    plan, _ = _plan_and_state(*_create_incident(factory))
    with (
        factory() as session,
        pytest.raises(RuntimeError, match="force rollback"),
        session.begin(),
    ):
        create_recovery_plan(
            session,
            plan,
            source=RecoveryPlanSource.SYSTEM,
            created_by="integration-test",
        )
        raise RuntimeError("force rollback")
    with factory() as session:
        assert session.get(RecoveryPlanEntity, plan.plan_id) is None


def test_concurrent_duplicate_creation_produces_one_plan() -> None:
    factory = _factory()
    plan, _ = _plan_and_state(*_create_incident(factory))
    barrier = Barrier(2)

    def create_once():
        with factory() as session, session.begin():
            barrier.wait(timeout=10)
            _, created = create_recovery_plan(
                session,
                plan,
                source=RecoveryPlanSource.SYSTEM,
                created_by="concurrency-test",
            )
            return created

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: create_once(), range(2)))
    assert sorted(results) == [False, True]
    with factory() as session:
        assert (
            len(
                session.scalars(
                    select(RecoveryPlanEntity).where(RecoveryPlanEntity.plan_id == plan.plan_id)
                ).all()
            )
            == 1
        )
        assert (
            len(
                session.scalars(
                    select(RecoveryLifecycleEvent).where(
                        RecoveryLifecycleEvent.plan_id == plan.plan_id
                    )
                ).all()
            )
            == 1
        )
