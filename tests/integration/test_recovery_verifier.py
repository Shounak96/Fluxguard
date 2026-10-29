import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Event
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from fluxguard.apps.worker.consumer import apply_event, parse_event, safe_text
from fluxguard.apps.worker.outbox import KafkaEventPublisher, event_bytes
from fluxguard.packages.common import recovery_policy, recovery_verifier
from fluxguard.packages.common.incidents import Detection, record_incident
from fluxguard.packages.common.models import (
    IncidentEvidence,
    IncidentSeverity,
    OrderProjection,
    OutboxEvent,
    ProcessedEvent,
    QuarantinedEvent,
)
from fluxguard.packages.common.recoveries import (
    create_recovery_plan,
    get_recovery_plan_history,
    recovery_evidence_from_row,
)
from fluxguard.packages.common.recovery_policy import build_recovery_plan
from fluxguard.packages.common.recovery_verifier import (
    RecoverySnapshotBusyError,
    load_authoritative_recovery_snapshot,
    verify_recovery_plan_dry_run,
)
from fluxguard.packages.contracts.recovery import (
    EligibilityStatus,
    ExpectedRecoveryEffect,
    KafkaCoordinate,
    RecoveryActionType,
    RecoveryLifecycleState,
    RecoveryPlanSource,
    RecoveryRejectionReason,
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


@dataclass(frozen=True)
class RecoveryFixture:
    incident_id: UUID
    evidence_id: UUID
    event_id: UUID
    order_id: UUID
    coordinate: KafkaCoordinate
    raw: bytes
    raw_hash: str


def _factory():
    return sessionmaker(
        bind=create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True),
        expire_on_commit=False,
    )


def _event_payload(
    event_id: UUID,
    order_id: UUID,
    *,
    schema_version: int = 1,
    total: str = "49.98",
    line_total: str = "49.98",
) -> dict:
    return {
        "event_id": str(event_id),
        "event_type": "orders.created.v1",
        "schema_version": schema_version,
        "aggregate_id": str(order_id),
        "occurred_at": "2026-01-01T00:00:00Z",
        "payload": {
            "order_id": str(order_id),
            "customer_id": "recovery-verifier",
            "items": [
                {
                    "product_id": "product-1",
                    "quantity": 2,
                    "unit_price": "24.99",
                    "line_total": line_total,
                }
            ],
            "currency": "USD",
            "total": total,
            "created_at": "2026-01-01T00:00:00Z",
        },
    }


def _raw(payload: dict) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _new_coordinate() -> KafkaCoordinate:
    return KafkaCoordinate(
        topic="orders.created.v1",
        partition=0,
        offset=1_000_000 + (uuid4().int % 1_000_000_000),
    )


def _create_source(
    factory,
    *,
    source: str,
    raw: bytes | None = None,
    event_id: UUID | None = None,
    order_id: UUID | None = None,
) -> RecoveryFixture:
    event_id = event_id or uuid4()
    order_id = order_id or uuid4()
    coordinate = _new_coordinate()
    if source == "outbox":
        payload = _event_payload(event_id, order_id)
        outbox = OutboxEvent(
            event_id=event_id,
            event_type="orders.created.v1",
            schema_version=1,
            aggregate_id=order_id,
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
            payload=payload["payload"],
        )
        raw = event_bytes(outbox)
    elif raw is None:
        raw = _raw(_event_payload(event_id, order_id))
    raw_hash = hashlib.sha256(raw).hexdigest()
    with factory() as session, session.begin():
        if source == "outbox":
            session.add(outbox)
        else:
            session.add(
                QuarantinedEvent(
                    topic=coordinate.topic,
                    partition=coordinate.partition,
                    offset=coordinate.offset,
                    message_key=str(order_id),
                    raw_payload=safe_text(raw),
                    error_type="RECOVERY_TEST_FIXTURE",
                    error_detail="deterministic recovery verifier fixture",
                )
            )
        incident, _ = record_incident(
            session,
            Detection(
                incident_type="RECOVERY_VERIFIER_TEST",
                severity=IncidentSeverity.MEDIUM,
                error_code="RECOVERY_VERIFIER_TEST",
                summary="recovery verifier integration fixture",
                source_component="recovery_verifier_test",
                fingerprint_scope={"scenario_id": str(uuid4())},
                evidence_type="RECOVERY_SOURCE",
                evidence_payload={"raw_sha256": raw_hash},
                event_id=event_id,
                topic=coordinate.topic,
                partition=coordinate.partition,
                offset=coordinate.offset,
            ),
        )
        session.flush()
        evidence_id = session.scalar(
            select(IncidentEvidence.evidence_id).where(
                IncidentEvidence.incident_id == incident.incident_id
            )
        )
    return RecoveryFixture(
        incident_id=incident.incident_id,
        evidence_id=evidence_id,
        event_id=event_id,
        order_id=order_id,
        coordinate=coordinate,
        raw=raw,
        raw_hash=raw_hash,
    )


def _target(fixture: RecoveryFixture, action: RecoveryActionType) -> RecoveryTarget:
    return RecoveryTarget(
        event_id=fixture.event_id,
        aggregate_id=fixture.order_id,
        kafka_coordinate=fixture.coordinate,
        outbox_event_id=(
            fixture.event_id if action == RecoveryActionType.RESCHEDULE_OUTBOX_EVENT else None
        ),
    )


def _persist_plan(factory, fixture: RecoveryFixture, action: RecoveryActionType):
    with factory() as session:
        evidence_row = session.get(IncidentEvidence, fixture.evidence_id)
        evidence = recovery_evidence_from_row(evidence_row)
    provisional_state = RecoveryStateSnapshot(
        incident_id=fixture.incident_id,
        evidence=(evidence,),
    )
    provisional = build_recovery_plan(
        action=action,
        incident_id=fixture.incident_id,
        target=_target(fixture, action),
        evidence_ids=[fixture.evidence_id],
        state=provisional_state,
        expected_event_sha256=fixture.raw_hash,
    )
    with factory() as session, session.begin():
        loaded = load_authoritative_recovery_snapshot(session, provisional)
    plan = build_recovery_plan(
        action=action,
        incident_id=fixture.incident_id,
        target=_target(fixture, action),
        evidence_ids=[fixture.evidence_id],
        state=loaded,
        expected_event_sha256=fixture.raw_hash,
    )
    with factory() as session, session.begin():
        create_recovery_plan(
            session,
            plan,
            source=RecoveryPlanSource.SYSTEM,
            created_by="recovery-verifier-test",
        )
    return plan


def _persist_projection(factory, fixture: RecoveryFixture, *, conflicting: bool = False) -> None:
    event = parse_event(fixture.raw)
    payload = event.payload.model_dump(mode="json")
    if conflicting:
        payload["customer_id"] = "conflicting-customer"
    with factory() as session, session.begin():
        if not conflicting:
            session.add(
                ProcessedEvent(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    aggregate_id=event.aggregate_id,
                )
            )
        session.add(
            OrderProjection(
                order_id=event.aggregate_id,
                source_event_id=event.event_id,
                customer_id=payload["customer_id"],
                currency=event.payload.currency,
                total=event.payload.total,
                item_count=2,
                payload=payload,
                created_at=event.payload.created_at,
            )
        )


def _business_snapshot(factory):
    with factory() as session:
        return recovery_verifier._business_snapshot(session)


def test_safe_replay_simulates_exact_writes_and_rolls_back() -> None:
    factory = _factory()
    fixture = _create_source(factory, source="outbox")
    plan = _persist_plan(factory, fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
    before = _business_snapshot(factory)
    result = verify_recovery_plan_dry_run(factory, plan.plan_id)
    after = _business_snapshot(factory)
    assert result.decision.status == EligibilityStatus.ELIGIBLE
    assert result.decision.expected_effect == ExpectedRecoveryEffect.APPLY_EVENT
    assert set(result.simulated_write_set) == {
        f"processed_events:{fixture.event_id}",
        f"order_projections:{fixture.order_id}",
    }
    assert result.business_state_rolled_back is True
    assert result.kafka_publications == 0
    assert before == after
    with factory() as session:
        assert session.get(ProcessedEvent, fixture.event_id) is None
        assert session.get(OrderProjection, fixture.order_id) is None


def test_duplicate_identical_event_is_a_non_mutating_noop() -> None:
    factory = _factory()
    fixture = _create_source(factory, source="outbox")
    _persist_projection(factory, fixture)
    plan = _persist_plan(factory, fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
    before = _business_snapshot(factory)
    result = verify_recovery_plan_dry_run(factory, plan.plan_id)
    assert result.decision.expected_effect == ExpectedRecoveryEffect.NO_OP_ALREADY_APPLIED
    assert result.simulated_write_set == ()
    assert _business_snapshot(factory) == before


def test_conflicting_projection_is_rejected_without_changes() -> None:
    factory = _factory()
    fixture = _create_source(factory, source="outbox")
    _persist_projection(factory, fixture, conflicting=True)
    plan = _persist_plan(factory, fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
    before = _business_snapshot(factory)
    result = verify_recovery_plan_dry_run(factory, plan.plan_id)
    assert result.decision.status == EligibilityStatus.INELIGIBLE
    assert result.decision.reasons == (RecoveryRejectionReason.CONFLICTING_PROJECTION,)
    assert _business_snapshot(factory) == before


@pytest.mark.parametrize(
    ("payload", "expected_reason"),
    [
        (
            lambda event_id, order_id: _raw(_event_payload(event_id, order_id, schema_version=99)),
            RecoveryRejectionReason.UNSUPPORTED_SCHEMA,
        ),
        (lambda _event_id, _order_id: b"not-json", RecoveryRejectionReason.MALFORMED_PAYLOAD),
        (
            lambda event_id, order_id: _raw(_event_payload(event_id, order_id, total="50.00")),
            RecoveryRejectionReason.INVALID_BUSINESS_INVARIANT,
        ),
    ],
)
def test_unsafe_replay_payloads_are_rejected(payload, expected_reason) -> None:
    factory = _factory()
    event_id, order_id = uuid4(), uuid4()
    fixture = _create_source(
        factory,
        source="quarantine",
        raw=payload(event_id, order_id),
        event_id=event_id,
        order_id=order_id,
    )
    plan = _persist_plan(factory, fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
    before = _business_snapshot(factory)
    result = verify_recovery_plan_dry_run(factory, plan.plan_id)
    assert result.decision.status == EligibilityStatus.INELIGIBLE
    assert result.decision.reasons == (expected_reason,)
    assert _business_snapshot(factory) == before


def test_missing_and_foreign_evidence_fail_closed_in_postgres_snapshot() -> None:
    factory = _factory()
    own = _create_source(factory, source="quarantine")
    foreign = _create_source(factory, source="quarantine")
    missing_id = uuid4()
    missing_seed = build_recovery_plan(
        action=RecoveryActionType.RETAIN_QUARANTINE,
        incident_id=own.incident_id,
        target=_target(own, RecoveryActionType.RETAIN_QUARANTINE),
        evidence_ids=[missing_id],
        state=RecoveryStateSnapshot(incident_id=own.incident_id),
    )
    with factory() as session, session.begin():
        missing_state = load_authoritative_recovery_snapshot(session, missing_seed)
    missing_plan = build_recovery_plan(
        action=RecoveryActionType.RETAIN_QUARANTINE,
        incident_id=own.incident_id,
        target=_target(own, RecoveryActionType.RETAIN_QUARANTINE),
        evidence_ids=[missing_id],
        state=missing_state,
    )
    missing = recovery_policy.evaluate_recovery_plan(missing_plan, missing_state)
    assert missing.status == EligibilityStatus.INSUFFICIENT_EVIDENCE
    assert missing.reasons == (RecoveryRejectionReason.MISSING_EVIDENCE,)

    foreign_seed = build_recovery_plan(
        action=RecoveryActionType.RETAIN_QUARANTINE,
        incident_id=own.incident_id,
        target=_target(own, RecoveryActionType.RETAIN_QUARANTINE),
        evidence_ids=[foreign.evidence_id],
        state=RecoveryStateSnapshot(incident_id=own.incident_id),
    )
    with factory() as session, session.begin():
        foreign_state = load_authoritative_recovery_snapshot(session, foreign_seed)
    foreign_plan = build_recovery_plan(
        action=RecoveryActionType.RETAIN_QUARANTINE,
        incident_id=own.incident_id,
        target=_target(own, RecoveryActionType.RETAIN_QUARANTINE),
        evidence_ids=[foreign.evidence_id],
        state=foreign_state,
    )
    foreign_decision = recovery_policy.evaluate_recovery_plan(foreign_plan, foreign_state)
    assert foreign_decision.status == EligibilityStatus.INELIGIBLE
    assert foreign_decision.reasons == (RecoveryRejectionReason.FOREIGN_EVIDENCE,)


def test_stale_state_and_policy_change_are_persisted_as_rejections(monkeypatch) -> None:
    factory = _factory()
    stale_fixture = _create_source(factory, source="outbox")
    stale_plan = _persist_plan(factory, stale_fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
    _persist_projection(factory, stale_fixture)
    stale = verify_recovery_plan_dry_run(factory, stale_plan.plan_id)
    assert stale.decision.reasons == (RecoveryRejectionReason.STALE_STATE,)
    assert stale.persisted_verification.state_digest != stale_plan.expected_state_digest

    policy_fixture = _create_source(factory, source="outbox")
    policy_plan = _persist_plan(factory, policy_fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
    monkeypatch.setattr(recovery_policy, "RECOVERY_POLICY_VERSION", "fluxguard.recovery.v2-test")
    changed_policy = verify_recovery_plan_dry_run(factory, policy_plan.plan_id)
    assert changed_policy.decision.reasons == (RecoveryRejectionReason.POLICY_VERSION_MISMATCH,)
    assert changed_policy.persisted_verification.verifier_version == "fluxguard.recovery.v2-test"


def test_outbox_reschedule_rolls_back_and_never_publishes(monkeypatch) -> None:
    factory = _factory()
    fixture = _create_source(factory, source="outbox")
    plan = _persist_plan(factory, fixture, RecoveryActionType.RESCHEDULE_OUTBOX_EVENT)

    def forbidden_publish(*_args, **_kwargs):
        raise AssertionError("dry run attempted Kafka publication")

    monkeypatch.setattr(KafkaEventPublisher, "publish", forbidden_publish)
    before = _business_snapshot(factory)
    result = verify_recovery_plan_dry_run(factory, plan.plan_id)
    assert result.decision.expected_effect == ExpectedRecoveryEffect.RESCHEDULE_OUTBOX
    assert result.simulated_write_set == (f"outbox_events:{fixture.event_id}",)
    assert result.kafka_publications == 0
    assert _business_snapshot(factory) == before
    with factory() as session:
        outbox = session.get(OutboxEvent, fixture.event_id)
        assert outbox.next_attempt_at is None
        assert outbox.published_at is None


def test_outbox_verification_does_not_wait_behind_publisher_lock() -> None:
    factory = _factory()
    fixture = _create_source(factory, source="outbox")
    plan = _persist_plan(factory, fixture, RecoveryActionType.RESCHEDULE_OUTBOX_EVENT)
    locked = Event()
    release = Event()

    def hold_publisher_lock():
        with factory() as session, session.begin():
            session.scalar(
                select(OutboxEvent)
                .where(OutboxEvent.event_id == fixture.event_id)
                .with_for_update()
            )
            locked.set()
            assert release.wait(timeout=5)

    with ThreadPoolExecutor(max_workers=1) as pool:
        holder = pool.submit(hold_publisher_lock)
        assert locked.wait(timeout=5)
        with pytest.raises(RecoverySnapshotBusyError):
            verify_recovery_plan_dry_run(factory, plan.plan_id)
        release.set()
        holder.result(timeout=5)
    with factory() as session:
        history = get_recovery_plan_history(session, plan.plan_id)
        assert history.plan.current_state == RecoveryLifecycleState.PROPOSED
        assert history.verifications == ()


@pytest.mark.parametrize(
    "action",
    [RecoveryActionType.RETAIN_QUARANTINE, RecoveryActionType.REQUEST_MANUAL_REVIEW],
)
def test_non_mutating_actions_leave_incident_and_quarantine_unchanged(action) -> None:
    factory = _factory()
    fixture = _create_source(factory, source="quarantine")
    plan = _persist_plan(factory, fixture, action)
    before = _business_snapshot(factory)
    result = verify_recovery_plan_dry_run(factory, plan.plan_id)
    assert result.decision.status == EligibilityStatus.ELIGIBLE
    assert result.simulated_write_set == ()
    assert _business_snapshot(factory) == before


def test_consumer_and_verifier_race_has_one_consumer_business_effect(monkeypatch) -> None:
    factory = _factory()
    fixture = _create_source(factory, source="outbox")
    plan = _persist_plan(factory, fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
    simulated = Event()
    release = Event()
    original_apply = recovery_verifier.apply_event_in_transaction

    def held_apply(session, event):
        applied = original_apply(session, event)
        simulated.set()
        assert release.wait(timeout=5)
        return applied

    monkeypatch.setattr(recovery_verifier, "apply_event_in_transaction", held_apply)
    event = parse_event(fixture.raw)
    with ThreadPoolExecutor(max_workers=2) as pool:
        verifier_future = pool.submit(verify_recovery_plan_dry_run, factory, plan.plan_id)
        assert simulated.wait(timeout=5)
        consumer_future = pool.submit(_apply_consumer_event, factory, event)
        release.set()
        verifier_result = verifier_future.result(timeout=10)
        assert consumer_future.result(timeout=10) is True
    assert verifier_result.business_state_rolled_back is True
    with factory() as session:
        assert session.get(ProcessedEvent, fixture.event_id) is not None
        assert session.get(OrderProjection, fixture.order_id) is not None


def _apply_consumer_event(factory, event) -> bool:
    with factory() as session:
        return apply_event(session, event)


def test_two_verifiers_use_nonblocking_target_lock(monkeypatch) -> None:
    factory = _factory()
    fixture = _create_source(factory, source="outbox")
    plan = _persist_plan(factory, fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
    entered = Event()
    release = Event()
    original_simulate = recovery_verifier._simulate_eligible_action

    def held_simulation(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=5)
        return original_simulate(*args, **kwargs)

    monkeypatch.setattr(recovery_verifier, "_simulate_eligible_action", held_simulation)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(verify_recovery_plan_dry_run, factory, plan.plan_id)
        assert entered.wait(timeout=5)
        second = pool.submit(verify_recovery_plan_dry_run, factory, plan.plan_id)
        with pytest.raises(RecoverySnapshotBusyError):
            second.result(timeout=5)
        release.set()
        assert first.result(timeout=10).decision.status == EligibilityStatus.ELIGIBLE
    with factory() as session:
        history = get_recovery_plan_history(session, plan.plan_id)
        assert len(history.verifications) == 1


def test_exception_rolls_back_simulation_and_does_not_persist_verification(monkeypatch) -> None:
    factory = _factory()
    fixture = _create_source(factory, source="outbox")
    plan = _persist_plan(factory, fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)

    def apply_then_fail(session, _plan, state, _decision):
        event = parse_event(state.authoritative_event.raw_event)
        recovery_verifier.apply_event_in_transaction(session, event)
        raise RuntimeError("controlled verifier failure")

    monkeypatch.setattr(recovery_verifier, "_simulate_eligible_action", apply_then_fail)
    before = _business_snapshot(factory)
    with pytest.raises(RuntimeError, match="controlled verifier failure"):
        verify_recovery_plan_dry_run(factory, plan.plan_id)
    assert _business_snapshot(factory) == before
    with factory() as session:
        history = get_recovery_plan_history(session, plan.plan_id)
        assert history.plan.current_state == RecoveryLifecycleState.PROPOSED
        assert history.verifications == ()
        assert session.get(ProcessedEvent, fixture.event_id) is None
        assert session.get(OrderProjection, fixture.order_id) is None
