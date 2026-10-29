import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from fluxguard.apps.api.main import create_app
from fluxguard.apps.api.recoveries import get_recovery_authorizer
from fluxguard.apps.worker.consumer import apply_event, parse_event
from fluxguard.apps.worker.outbox import event_bytes
from fluxguard.packages.common import recovery_policy, recovery_verifier
from fluxguard.packages.common.config import get_settings
from fluxguard.packages.common.db import get_session
from fluxguard.packages.common.incidents import Detection, record_incident
from fluxguard.packages.common.models import (
    IncidentEvidence,
    IncidentSeverity,
    OrderProjection,
    OutboxEvent,
    ProcessedEvent,
    QuarantinedEvent,
    RecoveryApproval,
    RecoveryExecutionEvent,
    RecoveryExecutionRequest,
    RecoveryLifecycleEvent,
    RecoveryVerification,
)
from fluxguard.packages.common.recoveries import (
    create_recovery_plan,
    get_recovery_plan_review,
    recovery_evidence_from_row,
)
from fluxguard.packages.common.recovery_approvals import (
    AuthenticatedRecoveryOperator,
    RecoveryApprovalConflictError,
    RecoveryApprovalExpiredError,
    RecoveryApprovalStaleError,
    RecoveryApprovalTransitionError,
    approve_recovery_plan,
)
from fluxguard.packages.common.recovery_executor import (
    RecoveryExecutionConflictError,
    RecoveryExecutionRejectedError,
    RecoveryExecutionStaleError,
    execute_recovery_plan,
)
from fluxguard.packages.common.recovery_policy import build_recovery_plan
from fluxguard.packages.common.recovery_verifier import (
    load_authoritative_recovery_snapshot,
    verify_recovery_plan_dry_run,
)
from fluxguard.packages.contracts.recovery import (
    KafkaCoordinate,
    RecoveryActionType,
    RecoveryExecutionStatus,
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


class TestAuthorizer:
    __test__ = False

    def __init__(self, subject: str = "operator:test") -> None:
        self.subject = subject

    def authenticate(self) -> AuthenticatedRecoveryOperator:
        return AuthenticatedRecoveryOperator(
            subject=self.subject,
            identity_provider="test-only",
        )

    def authorize(self, _plan_id, _execution_id) -> AuthenticatedRecoveryOperator:
        return self.authenticate()


def _factory():
    return sessionmaker(
        bind=create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True),
        expire_on_commit=False,
    )


def _create_verified_plan(
    factory,
    action: RecoveryActionType = RecoveryActionType.REPLAY_VERIFIED_EVENT,
    *,
    already_processed: bool = False,
):
    event_id = uuid4()
    order_id = uuid4()
    coordinate = KafkaCoordinate(
        topic="orders.created.v1",
        partition=0,
        offset=2_000_000 + uuid4().int % 1_000_000_000,
    )
    outbox = OutboxEvent(
        event_id=event_id,
        event_type="orders.created.v1",
        schema_version=1,
        aggregate_id=order_id,
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        payload={
            "order_id": str(order_id),
            "customer_id": "recovery-approval",
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
    )
    raw_hash = hashlib.sha256(event_bytes(outbox)).hexdigest()
    raw = event_bytes(outbox)
    with factory() as session, session.begin():
        session.add(outbox)
        if action == RecoveryActionType.RETAIN_QUARANTINE:
            session.add(
                QuarantinedEvent(
                    topic=coordinate.topic,
                    partition=coordinate.partition,
                    offset=coordinate.offset,
                    message_key=str(order_id),
                    raw_payload=raw.decode(),
                    error_type="RECOVERY_APPROVAL_TEST",
                    error_detail="retained for deterministic recovery test",
                )
            )
        incident, _ = record_incident(
            session,
            Detection(
                incident_type="RECOVERY_APPROVAL_TEST",
                severity=IncidentSeverity.MEDIUM,
                error_code="RECOVERY_APPROVAL_TEST",
                summary="recovery approval integration fixture",
                source_component="recovery_approval_test",
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
        evidence = session.scalar(
            select(IncidentEvidence).where(IncidentEvidence.incident_id == incident.incident_id)
        )
        incident_id = incident.incident_id
        evidence_id = evidence.evidence_id
    if already_processed:
        with factory() as session:
            apply_event(session, parse_event(raw))
    with factory() as session:
        evidence = recovery_evidence_from_row(session.get(IncidentEvidence, evidence_id))
    target = RecoveryTarget(
        event_id=event_id,
        aggregate_id=order_id,
        kafka_coordinate=coordinate,
        outbox_event_id=(
            event_id if action == RecoveryActionType.RESCHEDULE_OUTBOX_EVENT else None
        ),
    )
    provisional = build_recovery_plan(
        action=action,
        incident_id=incident_id,
        target=target,
        evidence_ids=[evidence_id],
        state=RecoveryStateSnapshot(incident_id=incident_id, evidence=(evidence,)),
        expected_event_sha256=raw_hash,
    )
    with factory() as session, session.begin():
        snapshot = load_authoritative_recovery_snapshot(session, provisional)
    plan = build_recovery_plan(
        action=action,
        incident_id=incident_id,
        target=target,
        evidence_ids=[evidence_id],
        state=snapshot,
        expected_event_sha256=raw_hash,
    )
    with factory() as session, session.begin():
        create_recovery_plan(
            session,
            plan,
            source=RecoveryPlanSource.SYSTEM,
            created_by="recovery-approval-test",
        )
    result = verify_recovery_plan_dry_run(factory, plan.plan_id)
    return plan, result, event_id, order_id


def _operator(subject: str = "operator:test") -> AuthenticatedRecoveryOperator:
    return AuthenticatedRecoveryOperator(subject=subject, identity_provider="test-only")


def _approve(factory, plan_id, key="approval-key-0001", **kwargs):
    with factory() as session:
        return approve_recovery_plan(
            session,
            plan_id,
            operator=_operator(),
            idempotency_key=key,
            **kwargs,
        )


def _execute(factory, plan, execution_id=None, **kwargs):
    return execute_recovery_plan(
        factory,
        plan.plan_id,
        execution_id or uuid4(),
        expected_plan_hash=plan.plan_hash,
        authorizer=TestAuthorizer(),
        **kwargs,
    )


def test_plan_inspection_and_test_authorized_approval_are_sanitized_and_non_mutating() -> None:
    factory = _factory()
    plan, dry_run, _, _ = _create_verified_plan(factory)
    with factory() as session:
        before = recovery_verifier._business_snapshot(session)
    approval = _approve(factory, plan.plan_id)
    with factory() as session:
        after = recovery_verifier._business_snapshot(session)
    with factory() as session:
        review = get_recovery_plan_review(session, plan.plan_id)
    assert approval.operator_subject == "operator:test"
    assert approval.verification_id == dry_run.persisted_verification.verification_id
    assert review.plan.current_state == RecoveryLifecycleState.APPROVED
    assert review.latest_verification is not None
    assert review.declared_write_set == dry_run.simulated_write_set
    assert review.postconditions == dry_run.postconditions
    assert [item.to_state for item in review.lifecycle_events][-2:] == [
        RecoveryLifecycleState.AWAITING_APPROVAL,
        RecoveryLifecycleState.APPROVED,
    ]
    assert before == after


def test_review_endpoint_is_development_gated_and_default_approval_is_unauthorized() -> None:
    factory = _factory()
    plan, _, _, _ = _create_verified_plan(factory)

    def sessions():
        with factory() as session:
            yield session

    settings = get_settings()
    original = settings.recovery_api_enabled
    settings.recovery_api_enabled = True
    app = create_app()
    app.dependency_overrides[get_session] = sessions
    try:
        with TestClient(app) as client:
            review = client.get(f"/api/v1/recovery-plans/{plan.plan_id}")
            evidence = client.get(f"/api/v1/recovery-plans/{plan.plan_id}/evidence")
            verification = client.get(f"/api/v1/recovery-plans/{plan.plan_id}/verification/latest")
            effects = client.get(f"/api/v1/recovery-plans/{plan.plan_id}/expected-effects")
            history = client.get(f"/api/v1/recovery-plans/{plan.plan_id}/history")
            denied = client.post(
                f"/api/v1/recovery-plans/{plan.plan_id}/approval",
                json={"idempotency_key": "api-approval-key"},
            )
        assert review.status_code == 200
        assert review.json()["evidence"][0]["incident_id"] == str(plan.incident_id)
        assert review.json()["declared_write_set"]
        assert evidence.status_code == 200
        assert evidence.json()[0]["incident_id"] == str(plan.incident_id)
        assert verification.status_code == 200
        assert verification.json()["decision"] == "ELIGIBLE"
        assert effects.status_code == 200
        assert effects.json()["declared_write_set"]
        assert history.status_code == 200
        assert history.json()[-1]["to_state"] == "VERIFIED"
        assert denied.status_code == 403
        assert denied.json()["detail"]["code"] == "RECOVERY_APPROVAL_DISABLED"
    finally:
        settings.recovery_api_enabled = original


def test_operator_identity_cannot_be_forged_in_request_payload() -> None:
    factory = _factory()
    plan, _, _, _ = _create_verified_plan(factory)

    def sessions():
        with factory() as session:
            yield session

    settings = get_settings()
    original = settings.recovery_api_enabled
    settings.recovery_api_enabled = True
    app = create_app()
    app.dependency_overrides[get_session] = sessions
    app.dependency_overrides[get_recovery_authorizer] = lambda: TestAuthorizer()
    try:
        with TestClient(app) as client:
            response = client.post(
                f"/api/v1/recovery-plans/{plan.plan_id}/approval",
                json={
                    "idempotency_key": "api-approval-key",
                    "operator_subject": "forged-admin",
                },
            )
        assert response.status_code == 422
        with factory() as session:
            assert (
                session.scalar(
                    select(RecoveryApproval).where(RecoveryApproval.plan_id == plan.plan_id)
                )
                is None
            )
    finally:
        settings.recovery_api_enabled = original


def test_duplicate_approval_is_idempotent_and_conflicting_request_is_rejected() -> None:
    factory = _factory()
    plan, _, _, _ = _create_verified_plan(factory)
    first = _approve(factory, plan.plan_id)
    second = _approve(factory, plan.plan_id)
    assert second == first
    with pytest.raises(RecoveryApprovalConflictError):
        _approve(factory, plan.plan_id, key="different-key-0002")
    with factory() as session:
        lifecycle_count = len(get_recovery_plan_review(session, plan.plan_id).lifecycle_events)
    assert lifecycle_count == 4


def test_concurrent_conflicting_approvals_allow_exactly_one() -> None:
    factory = _factory()
    plan, _, _, _ = _create_verified_plan(factory)

    def attempt(key: str, subject: str):
        try:
            with factory() as session:
                return approve_recovery_plan(
                    session,
                    plan.plan_id,
                    operator=_operator(subject),
                    idempotency_key=key,
                )
        except RecoveryApprovalConflictError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(
            pool.map(
                lambda item: attempt(*item),
                [("concurrent-key-1", "operator:one"), ("concurrent-key-2", "operator:two")],
            )
        )
    assert sum(not isinstance(item, Exception) for item in outcomes) == 1
    assert sum(isinstance(item, RecoveryApprovalConflictError) for item in outcomes) == 1


def test_expired_verification_and_approval_append_expired_lifecycle() -> None:
    factory = _factory()
    plan, result, _, _ = _create_verified_plan(factory)
    with pytest.raises(RecoveryApprovalExpiredError):
        _approve(
            factory,
            plan.plan_id,
            now=result.persisted_verification.verified_at + timedelta(minutes=16),
        )
    with factory() as session:
        assert get_recovery_plan_review(session, plan.plan_id).plan.current_state == (
            RecoveryLifecycleState.EXPIRED
        )
    with pytest.raises(RecoveryApprovalTransitionError):
        _approve(factory, plan.plan_id, key="approval-after-expiry")

    plan2, result2, _, _ = _create_verified_plan(factory)
    approval = _approve(
        factory,
        plan2.plan_id,
        now=result2.persisted_verification.verified_at + timedelta(seconds=1),
    )
    with pytest.raises(RecoveryApprovalExpiredError):
        _approve(factory, plan2.plan_id, now=approval.expires_at)
    with factory() as session:
        assert get_recovery_plan_review(session, plan2.plan_id).plan.current_state == (
            RecoveryLifecycleState.EXPIRED
        )


def test_stale_state_and_version_mismatch_fail_closed() -> None:
    factory = _factory()
    plan, _, event_id, _ = _create_verified_plan(factory)
    with factory() as session, session.begin():
        outbox = session.get(OutboxEvent, event_id)
        outbox.attempts += 1
    with pytest.raises(RecoveryApprovalStaleError):
        _approve(factory, plan.plan_id)
    with factory() as session:
        assert get_recovery_plan_review(session, plan.plan_id).plan.current_state == (
            RecoveryLifecycleState.EXPIRED
        )

    plan2, _, _, _ = _create_verified_plan(factory)
    with factory() as session, session.begin():
        evidence = session.get(IncidentEvidence, plan2.evidence_ids[0])
        evidence.payload = {**evidence.payload, "raw_sha256": "0" * 64}
    with pytest.raises(RecoveryApprovalStaleError):
        _approve(factory, plan2.plan_id)

    plan3, _, _, _ = _create_verified_plan(factory)
    original = recovery_policy.RECOVERY_POLICY_VERSION
    recovery_policy.RECOVERY_POLICY_VERSION = "fluxguard.recovery.v-next"
    try:
        with pytest.raises(RecoveryApprovalStaleError):
            _approve(factory, plan3.plan_id)
    finally:
        recovery_policy.RECOVERY_POLICY_VERSION = original


def test_approval_and_lifecycle_rows_remain_append_only() -> None:
    factory = _factory()
    plan, _, _, _ = _create_verified_plan(factory)
    approval = _approve(factory, plan.plan_id)
    with factory() as session:
        with pytest.raises(DBAPIError):
            session.execute(
                update(RecoveryApproval)
                .where(RecoveryApproval.approval_id == approval.approval_id)
                .values(operator_subject="changed")
            )
            session.commit()
        session.rollback()
        with pytest.raises(DBAPIError):
            session.execute(
                delete(RecoveryLifecycleEvent).where(RecoveryLifecycleEvent.plan_id == plan.plan_id)
            )
            session.commit()


def test_verification_binding_cannot_be_silently_changed() -> None:
    factory = _factory()
    plan, result, _, _ = _create_verified_plan(factory)
    _approve(factory, plan.plan_id)
    with factory() as session:
        approval = session.scalar(
            select(RecoveryApproval).where(RecoveryApproval.plan_id == plan.plan_id)
        )
        verification = session.get(
            RecoveryVerification, result.persisted_verification.verification_id
        )
        assert approval.verification_id == verification.verification_id
        assert approval.state_digest == verification.state_digest
        assert approval.verifier_version == verification.verifier_version
        assert approval.plan_hash == plan.plan_hash
        assert approval.evidence_digest == plan.evidence_digest


def test_executor_applies_approved_replay_once_and_is_idempotent() -> None:
    factory = _factory()
    plan, _, event_id, order_id = _create_verified_plan(factory)
    _approve(factory, plan.plan_id)
    execution_id = uuid4()
    first = _execute(factory, plan, execution_id)
    second = _execute(factory, plan, execution_id)
    assert first == second
    assert first.status == RecoveryExecutionStatus.SUCCEEDED
    assert set(first.applied_write_set) == {
        f"processed_events:{event_id}",
        f"order_projections:{order_id}",
    }
    with factory() as session:
        assert session.get(ProcessedEvent, event_id) is not None
        assert session.get(OrderProjection, order_id) is not None
        assert (
            session.scalar(
                select(RecoveryExecutionRequest).where(
                    RecoveryExecutionRequest.execution_id == execution_id
                )
            )
            is not None
        )
    with pytest.raises(RecoveryExecutionConflictError):
        execute_recovery_plan(
            factory,
            uuid4(),
            execution_id,
            expected_plan_hash=plan.plan_hash,
            authorizer=TestAuthorizer(),
        )


def test_executor_reschedules_outbox_without_publication() -> None:
    factory = _factory()
    plan, _, event_id, _ = _create_verified_plan(
        factory, RecoveryActionType.RESCHEDULE_OUTBOX_EVENT
    )
    _approve(factory, plan.plan_id)
    result = _execute(factory, plan)
    assert result.status == RecoveryExecutionStatus.SUCCEEDED
    assert result.applied_write_set == (f"outbox_events:{event_id}",)
    with factory() as session:
        outbox = session.get(OutboxEvent, event_id)
        assert outbox.next_attempt_at == outbox.occurred_at
        assert outbox.published_at is None


@pytest.mark.parametrize(
    "action",
    [RecoveryActionType.RETAIN_QUARANTINE, RecoveryActionType.REQUEST_MANUAL_REVIEW],
)
def test_executor_non_mutating_actions_require_trusted_code_but_no_approval(action) -> None:
    factory = _factory()
    plan, _, _, _ = _create_verified_plan(factory, action)
    with factory() as session:
        before = recovery_verifier._business_snapshot(session)
    result = _execute(factory, plan)
    with factory() as session:
        after = recovery_verifier._business_snapshot(session)
    assert result.status == RecoveryExecutionStatus.NO_OP
    assert result.approval_id is None
    assert result.applied_write_set == ()
    assert before == after


def test_executor_verified_duplicate_is_noop() -> None:
    factory = _factory()
    plan, _, event_id, order_id = _create_verified_plan(factory, already_processed=True)
    _approve(factory, plan.plan_id)
    with factory() as session:
        marker_before = session.get(ProcessedEvent, event_id).delivery_count
    result = _execute(factory, plan)
    assert result.status == RecoveryExecutionStatus.NO_OP
    with factory() as session:
        assert session.get(ProcessedEvent, event_id).delivery_count == marker_before
        assert session.get(OrderProjection, order_id) is not None


def test_executor_rejects_unapproved_expired_stale_and_version_mismatched_plans() -> None:
    factory = _factory()
    unapproved, _, _, _ = _create_verified_plan(factory)
    with pytest.raises(RecoveryExecutionRejectedError):
        _execute(factory, unapproved)

    expired, verification, _, _ = _create_verified_plan(factory)
    approval = _approve(
        factory,
        expired.plan_id,
        now=verification.persisted_verification.verified_at + timedelta(seconds=1),
    )
    with pytest.raises(RecoveryExecutionRejectedError):
        _execute(factory, expired, now=approval.expires_at)

    stale, _, event_id, _ = _create_verified_plan(factory)
    _approve(factory, stale.plan_id)
    with factory() as session, session.begin():
        session.get(OutboxEvent, event_id).attempts += 1
    with pytest.raises(RecoveryExecutionStaleError):
        _execute(factory, stale)

    mismatched, _, _, _ = _create_verified_plan(factory)
    _approve(factory, mismatched.plan_id)
    original = recovery_policy.RECOVERY_POLICY_VERSION
    recovery_policy.RECOVERY_POLICY_VERSION = "fluxguard.recovery.v-next"
    try:
        with pytest.raises(RecoveryExecutionRejectedError):
            _execute(factory, mismatched)
    finally:
        recovery_policy.RECOVERY_POLICY_VERSION = original


def test_concurrent_executors_produce_at_most_one_business_effect() -> None:
    factory = _factory()
    plan, _, event_id, order_id = _create_verified_plan(factory)
    _approve(factory, plan.plan_id)

    def attempt():
        try:
            return _execute(factory, plan)
        except (RecoveryExecutionConflictError, RecoveryExecutionRejectedError) as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: attempt(), range(2)))
    assert sum(not isinstance(item, Exception) for item in outcomes) == 1
    assert (
        sum(
            isinstance(item, RecoveryExecutionConflictError | RecoveryExecutionRejectedError)
            for item in outcomes
        )
        == 1
    )
    with factory() as session:
        assert (
            session.scalar(select(ProcessedEvent).where(ProcessedEvent.event_id == event_id))
            is not None
        )
        assert (
            session.scalar(select(OrderProjection).where(OrderProjection.order_id == order_id))
            is not None
        )


def test_executor_rolls_back_exceptions_and_persists_sanitized_failure(
    monkeypatch,
) -> None:
    factory = _factory()
    plan, _, event_id, order_id = _create_verified_plan(factory)
    _approve(factory, plan.plan_id)
    execution_id = uuid4()
    original = recovery_verifier._simulate_eligible_action

    def apply_then_fail(session, selected_plan, state, decision):
        original(session, selected_plan, state, decision)
        raise RuntimeError("sensitive fixture detail must not be persisted")

    monkeypatch.setattr(recovery_verifier, "_simulate_eligible_action", apply_then_fail)
    with pytest.raises(RuntimeError):
        _execute(factory, plan, execution_id)
    with factory() as session:
        assert session.get(ProcessedEvent, event_id) is None
        assert session.get(OrderProjection, order_id) is None
        events = session.scalars(
            select(RecoveryExecutionEvent)
            .where(RecoveryExecutionEvent.execution_id == execution_id)
            .order_by(RecoveryExecutionEvent.sequence_number)
        ).all()
        assert [event.status for event in events] == ["STARTED", "FAILED"]
        assert events[-1].error_code == "RuntimeError"
        assert "sensitive" not in events[-1].error_message


def test_execution_audit_is_append_only() -> None:
    factory = _factory()
    plan, _, _, _ = _create_verified_plan(factory)
    _approve(factory, plan.plan_id)
    result = _execute(factory, plan)
    with factory() as session, pytest.raises(DBAPIError):
        session.execute(
            update(RecoveryExecutionEvent)
            .where(RecoveryExecutionEvent.execution_id == result.execution_id)
            .values(reason_code="changed")
        )
        session.commit()


def test_consumer_executor_race_has_one_business_effect(monkeypatch) -> None:
    factory = _factory()
    plan, _, event_id, order_id = _create_verified_plan(factory)
    _approve(factory, plan.plan_id)
    ready = Event()
    release = Event()
    original_lock = recovery_verifier._acquire_target_locks

    def pause_after_lock(session, selected_plan):
        original_lock(session, selected_plan)
        ready.set()
        assert release.wait(10)

    monkeypatch.setattr(recovery_verifier, "_acquire_target_locks", pause_after_lock)

    def consume(raw):
        with factory() as session:
            return apply_event(session, parse_event(raw))

    with ThreadPoolExecutor(max_workers=2) as pool:
        execution = pool.submit(_execute, factory, plan)
        assert ready.wait(10)
        with factory() as session:
            raw = event_bytes(session.get(OutboxEvent, event_id))
        consumer = pool.submit(consume, raw)
        assert consumer.result(timeout=10) is True
        release.set()
        with pytest.raises(RecoveryExecutionConflictError):
            execution.result(timeout=10)
    with factory() as session:
        assert session.get(ProcessedEvent, event_id) is not None
        assert session.get(OrderProjection, order_id) is not None


def test_publisher_executor_race_is_a_typed_conflict_without_executor_publish() -> None:
    factory = _factory()
    plan, _, event_id, _ = _create_verified_plan(
        factory, RecoveryActionType.RESCHEDULE_OUTBOX_EVENT
    )
    _approve(factory, plan.plan_id)
    ready = Event()
    release = Event()

    def hold_publisher_row_lock():
        with factory() as session, session.begin():
            session.scalar(
                select(OutboxEvent).where(OutboxEvent.event_id == event_id).with_for_update()
            )
            ready.set()
            assert release.wait(10)

    with ThreadPoolExecutor(max_workers=2) as pool:
        publication = pool.submit(hold_publisher_row_lock)
        assert ready.wait(10)
        with pytest.raises(RecoveryExecutionConflictError):
            _execute(factory, plan)
        release.set()
        publication.result(timeout=10)
    with factory() as session:
        outbox = session.get(OutboxEvent, event_id)
        assert outbox.published_at is None
        assert outbox.next_attempt_at is None
