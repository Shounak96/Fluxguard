import hashlib
import json
from contextlib import suppress
from datetime import datetime
from typing import Protocol
from uuid import UUID, uuid4

from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from fluxguard.packages.common import recovery_policy, recovery_verifier
from fluxguard.packages.common.models import (
    RecoveryApproval,
    RecoveryExecutionEvent,
    RecoveryExecutionRequest,
    RecoveryLifecycleEvent,
    RecoveryPlanEntity,
    RecoveryVerification,
)
from fluxguard.packages.common.recovery_approvals import AuthenticatedRecoveryOperator
from fluxguard.packages.common.recovery_policy import (
    recovery_evidence_digest,
    recovery_plan_identity_is_valid,
)
from fluxguard.packages.common.time import utc_now
from fluxguard.packages.contracts.recovery import (
    EligibilityStatus,
    ExpectedRecoveryEffect,
    RecoveryActionType,
    RecoveryExecutionEventView,
    RecoveryExecutionResult,
    RecoveryExecutionStatus,
    RecoveryLifecycleState,
    RecoveryPlan,
    RecoveryPlanSource,
)

_MUTATING_ACTIONS = frozenset(
    {
        RecoveryActionType.REPLAY_VERIFIED_EVENT,
        RecoveryActionType.RESCHEDULE_OUTBOX_EVENT,
    }
)


class RecoveryExecutionAuthorizer(Protocol):
    def authorize(self, plan_id: UUID, execution_id: UUID) -> AuthenticatedRecoveryOperator: ...


class RecoveryExecutionError(RuntimeError):
    code = "RECOVERY_EXECUTION_ERROR"


class RecoveryExecutionDisabledError(RecoveryExecutionError):
    code = "RECOVERY_EXECUTION_DISABLED"


class RecoveryExecutionConflictError(RecoveryExecutionError):
    code = "RECOVERY_EXECUTION_CONFLICT"


class RecoveryExecutionRejectedError(RecoveryExecutionError):
    code = "RECOVERY_EXECUTION_REJECTED"


class RecoveryExecutionStaleError(RecoveryExecutionError):
    code = "RECOVERY_EXECUTION_STALE"


class DisabledRecoveryExecutionAuthorizer:
    def authorize(self, plan_id: UUID, execution_id: UUID) -> AuthenticatedRecoveryOperator:
        del plan_id, execution_id
        raise RecoveryExecutionDisabledError(
            "recovery execution is disabled because no trusted authorizer is configured"
        )


def _request_digest(plan_id: UUID, expected_plan_hash: str) -> str:
    if len(expected_plan_hash) != 64 or any(
        character not in "0123456789abcdef" for character in expected_plan_hash
    ):
        raise ValueError("expected_plan_hash must be a lowercase SHA-256 digest")
    payload = json.dumps(
        {"plan_id": str(plan_id), "plan_hash": expected_plan_hash},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _event_view(event: RecoveryExecutionEvent) -> RecoveryExecutionEventView:
    return RecoveryExecutionEventView(
        event_id=event.event_id,
        execution_id=event.execution_id,
        plan_id=event.plan_id,
        sequence_number=event.sequence_number,
        status=RecoveryExecutionStatus(event.status),
        reason_code=event.reason_code,
        occurred_at=event.occurred_at,
        pre_state_digest=event.pre_state_digest,
        post_state_digest=event.post_state_digest,
        applied_write_set=tuple(event.applied_write_set),
        error_code=event.error_code,
    )


def _load_result(session: Session, execution_id: UUID) -> RecoveryExecutionResult | None:
    request = session.get(RecoveryExecutionRequest, execution_id)
    if request is None:
        return None
    events = session.scalars(
        select(RecoveryExecutionEvent)
        .where(RecoveryExecutionEvent.execution_id == execution_id)
        .order_by(RecoveryExecutionEvent.sequence_number)
    ).all()
    terminal = next(
        (
            event
            for event in reversed(events)
            if event.status
            in {
                RecoveryExecutionStatus.SUCCEEDED.value,
                RecoveryExecutionStatus.NO_OP.value,
                RecoveryExecutionStatus.FAILED.value,
            }
        ),
        None,
    )
    if terminal is None:
        raise RecoveryExecutionConflictError("execution request has no terminal audit event")
    return RecoveryExecutionResult(
        execution_id=request.execution_id,
        plan_id=request.plan_id,
        action=RecoveryActionType(request.action_type),
        status=RecoveryExecutionStatus(terminal.status),
        approval_id=request.approval_id,
        applied_write_set=tuple(terminal.applied_write_set),
        pre_state_digest=terminal.pre_state_digest,
        post_state_digest=terminal.post_state_digest,
        events=tuple(_event_view(event) for event in events),
    )


def _assert_same_request(
    request: RecoveryExecutionRequest,
    plan_id: UUID,
    request_digest: str,
    operator: AuthenticatedRecoveryOperator,
) -> None:
    if (
        request.plan_id != plan_id
        or request.request_digest != request_digest
        or request.actor_subject != operator.subject
        or request.identity_provider != operator.identity_provider
    ):
        raise RecoveryExecutionConflictError(
            "execution ID already exists with different immutable content"
        )


def _latest_lifecycle_for_update(session: Session, plan_id: UUID) -> RecoveryLifecycleEvent:
    event = session.scalar(
        select(RecoveryLifecycleEvent)
        .where(RecoveryLifecycleEvent.plan_id == plan_id)
        .order_by(RecoveryLifecycleEvent.sequence_number.desc())
        .limit(1)
        .with_for_update()
    )
    if event is None:
        raise RecoveryExecutionRejectedError("recovery plan has no lifecycle history")
    return event


def _append_lifecycle(
    session: Session,
    current: RecoveryLifecycleEvent,
    destination: RecoveryLifecycleState,
    reason_code: str,
    operator: AuthenticatedRecoveryOperator,
    now: datetime,
) -> RecoveryLifecycleEvent:
    allowed = {
        (RecoveryLifecycleState.APPROVED, RecoveryLifecycleState.EXECUTING),
        (RecoveryLifecycleState.VERIFIED, RecoveryLifecycleState.EXECUTING),
        (RecoveryLifecycleState.EXECUTING, RecoveryLifecycleState.SUCCEEDED),
        (RecoveryLifecycleState.EXECUTING, RecoveryLifecycleState.NO_OP),
    }
    transition = (RecoveryLifecycleState(current.to_state), destination)
    if transition not in allowed:
        raise RecoveryExecutionRejectedError(
            f"execution transition {transition[0].value} -> {destination.value} is not allowed"
        )
    event = RecoveryLifecycleEvent(
        event_id=uuid4(),
        plan_id=current.plan_id,
        sequence_number=current.sequence_number + 1,
        from_state=current.to_state,
        to_state=destination.value,
        reason_code=reason_code,
        actor_type=RecoveryPlanSource.OPERATOR.value,
        actor_id=operator.subject,
        occurred_at=now,
    )
    session.add(event)
    session.flush()
    return event


def _append_execution_event(
    session: Session,
    request: RecoveryExecutionRequest,
    sequence_number: int,
    status: RecoveryExecutionStatus,
    reason_code: str,
    now: datetime,
    *,
    pre_state_digest: str | None = None,
    post_state_digest: str | None = None,
    write_set: tuple[str, ...] = (),
    error_code: str | None = None,
    error_message: str | None = None,
) -> RecoveryExecutionEvent:
    event = RecoveryExecutionEvent(
        event_id=uuid4(),
        execution_id=request.execution_id,
        plan_id=request.plan_id,
        sequence_number=sequence_number,
        status=status.value,
        reason_code=reason_code,
        occurred_at=now,
        pre_state_digest=pre_state_digest,
        post_state_digest=post_state_digest,
        applied_write_set=list(write_set),
        error_code=error_code,
        error_message=error_message,
    )
    session.add(event)
    return event


def _validate_approval(
    plan: RecoveryPlan,
    verification: RecoveryVerification,
    approval: RecoveryApproval | None,
    now: datetime,
) -> RecoveryApproval | None:
    if plan.action not in _MUTATING_ACTIONS:
        return None
    if approval is None:
        raise RecoveryExecutionRejectedError("mutating recovery requires persisted approval")
    if now >= approval.expires_at:
        raise RecoveryExecutionRejectedError("recovery approval has expired")
    if (
        approval.plan_id != plan.plan_id
        or approval.verification_id != verification.verification_id
        or approval.plan_hash != plan.plan_hash
        or approval.evidence_digest != plan.evidence_digest
        or approval.state_digest != verification.state_digest
        or approval.policy_version != plan.policy_version
        or approval.verifier_version != verification.verifier_version
    ):
        raise RecoveryExecutionRejectedError("recovery approval binding is invalid")
    return approval


def _record_failure(
    session_factory: sessionmaker[Session],
    *,
    execution_id: UUID,
    plan_id: UUID,
    expected_plan_hash: str,
    request_digest: str,
    operator: AuthenticatedRecoveryOperator,
    error: Exception,
    now: datetime,
) -> None:
    with session_factory() as session, session.begin():
        existing = session.get(RecoveryExecutionRequest, execution_id)
        if existing is not None:
            _assert_same_request(existing, plan_id, request_digest, operator)
            return
        entity = session.get(RecoveryPlanEntity, plan_id)
        if entity is None:
            return
        plan = RecoveryPlan.model_validate(entity.canonical_inputs)
        approval = session.scalar(
            select(RecoveryApproval).where(RecoveryApproval.plan_id == plan_id)
        )
        request = RecoveryExecutionRequest(
            execution_id=execution_id,
            plan_id=plan_id,
            approval_id=approval.approval_id if approval else None,
            request_digest=request_digest,
            plan_hash=expected_plan_hash,
            action_type=plan.action.value,
            actor_subject=operator.subject,
            identity_provider=operator.identity_provider,
            requested_at=now,
        )
        session.add(request)
        session.flush()
        _append_execution_event(
            session,
            request,
            1,
            RecoveryExecutionStatus.STARTED,
            "EXECUTION_ATTEMPTED",
            now,
        )
        _append_execution_event(
            session,
            request,
            2,
            RecoveryExecutionStatus.FAILED,
            "EXECUTION_ROLLED_BACK",
            now,
            error_code=getattr(error, "code", type(error).__name__)[:100],
            error_message="recovery execution failed safely after rollback",
        )


def execute_recovery_plan(
    session_factory: sessionmaker[Session],
    plan_id: UUID,
    execution_id: UUID,
    *,
    expected_plan_hash: str,
    authorizer: RecoveryExecutionAuthorizer,
    now: datetime | None = None,
) -> RecoveryExecutionResult:
    operator = authorizer.authorize(plan_id, execution_id)
    executed_at = now or utc_now()
    request_digest = _request_digest(plan_id, expected_plan_hash)
    with session_factory() as read_session:
        existing = read_session.get(RecoveryExecutionRequest, execution_id)
        if existing is not None:
            _assert_same_request(existing, plan_id, request_digest, operator)
            result = _load_result(read_session, execution_id)
            if result is None:
                raise RecoveryExecutionConflictError("execution audit is incomplete")
            return result

    try:
        with session_factory() as session, session.begin():
            session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            session.execute(text("SET LOCAL lock_timeout = '500ms'"))
            session.execute(text("SET LOCAL statement_timeout = '5s'"))
            entity = session.scalar(
                select(RecoveryPlanEntity)
                .where(RecoveryPlanEntity.plan_id == plan_id)
                .with_for_update()
            )
            if entity is None:
                raise LookupError("recovery plan not found")
            plan = RecoveryPlan.model_validate(entity.canonical_inputs)
            if (
                entity.plan_hash != expected_plan_hash
                or plan.plan_hash != expected_plan_hash
                or not recovery_plan_identity_is_valid(plan)
            ):
                raise RecoveryExecutionRejectedError("recovery plan identity is invalid")
            current = _latest_lifecycle_for_update(session, plan_id)
            required_state = (
                RecoveryLifecycleState.APPROVED
                if plan.action in _MUTATING_ACTIONS
                else RecoveryLifecycleState.VERIFIED
            )
            if current.to_state != required_state.value:
                raise RecoveryExecutionRejectedError(
                    f"plan is not executable from lifecycle state {current.to_state}"
                )
            verification = session.scalar(
                select(RecoveryVerification).where(RecoveryVerification.plan_id == plan_id)
            )
            if (
                verification is None
                or verification.decision != EligibilityStatus.ELIGIBLE.value
                or verification.verifier_version != recovery_policy.RECOVERY_POLICY_VERSION
                or plan.policy_version != recovery_policy.RECOVERY_POLICY_VERSION
            ):
                raise RecoveryExecutionRejectedError(
                    "eligible current-version verification is required"
                )
            approval = _validate_approval(
                plan,
                verification,
                session.scalar(select(RecoveryApproval).where(RecoveryApproval.plan_id == plan_id)),
                executed_at,
            )
            recovery_verifier._acquire_target_locks(session, plan)
            state = recovery_verifier.load_authoritative_recovery_snapshot(session, plan)
            state_digest = recovery_policy.recovery_state_digest(state)
            if (
                state_digest != plan.expected_state_digest
                or state_digest != verification.state_digest
                or recovery_evidence_digest(state.evidence) != plan.evidence_digest
            ):
                raise RecoveryExecutionStaleError(
                    "authoritative recovery state or evidence changed after verification"
                )
            decision = recovery_policy.evaluate_recovery_plan(plan, state)
            if (
                decision.status != EligibilityStatus.ELIGIBLE
                or decision.expected_effect != ExpectedRecoveryEffect(verification.expected_effect)
            ):
                raise RecoveryExecutionRejectedError(
                    "current deterministic eligibility does not match verification"
                )
            request = RecoveryExecutionRequest(
                execution_id=execution_id,
                plan_id=plan_id,
                approval_id=approval.approval_id if approval else None,
                request_digest=request_digest,
                plan_hash=plan.plan_hash,
                action_type=plan.action.value,
                actor_subject=operator.subject,
                identity_provider=operator.identity_provider,
                requested_at=executed_at,
            )
            session.add(request)
            session.flush()
            _append_execution_event(
                session,
                request,
                1,
                RecoveryExecutionStatus.STARTED,
                "EXECUTION_STARTED",
                executed_at,
                pre_state_digest=state_digest,
            )
            write_set, postconditions = recovery_verifier._simulate_eligible_action(
                session, plan, state, decision
            )
            if write_set != tuple(verification.simulated_write_set):
                raise RecoveryExecutionRejectedError(
                    "actual execution write set differs from verified declaration"
                )
            if postconditions != tuple(verification.postconditions):
                raise RecoveryExecutionRejectedError(
                    "actual execution postconditions differ from verification"
                )
            post_state = recovery_verifier.load_authoritative_recovery_snapshot(session, plan)
            post_digest = recovery_policy.recovery_state_digest(post_state)
            executing = _append_lifecycle(
                session,
                current,
                RecoveryLifecycleState.EXECUTING,
                "EXECUTION_STARTED",
                operator,
                executed_at,
            )
            is_noop = not write_set
            terminal_status = (
                RecoveryExecutionStatus.NO_OP if is_noop else RecoveryExecutionStatus.SUCCEEDED
            )
            terminal_state = (
                RecoveryLifecycleState.NO_OP if is_noop else RecoveryLifecycleState.SUCCEEDED
            )
            _append_lifecycle(
                session,
                executing,
                terminal_state,
                "EXECUTION_NO_OP" if is_noop else "EXECUTION_SUCCEEDED",
                operator,
                executed_at,
            )
            _append_execution_event(
                session,
                request,
                2,
                terminal_status,
                "EXECUTION_NO_OP" if is_noop else "EXECUTION_COMMITTED",
                executed_at,
                pre_state_digest=state_digest,
                post_state_digest=post_digest,
                write_set=write_set,
            )
            session.flush()
        with session_factory() as session:
            result = _load_result(session, execution_id)
            if result is None:
                raise RecoveryExecutionError("committed execution audit is missing")
            return result
    except (DBAPIError, recovery_verifier.RecoverySnapshotBusyError) as exc:
        with suppress(IntegrityError):
            _record_failure(
                session_factory,
                execution_id=execution_id,
                plan_id=plan_id,
                expected_plan_hash=expected_plan_hash,
                request_digest=request_digest,
                operator=operator,
                error=exc,
                now=executed_at,
            )
        raise RecoveryExecutionConflictError(
            "recovery target changed or was locked by a concurrent actor"
        ) from exc
    except Exception as exc:
        with suppress(IntegrityError):
            _record_failure(
                session_factory,
                execution_id=execution_id,
                plan_id=plan_id,
                expected_plan_hash=expected_plan_hash,
                request_digest=request_digest,
                operator=operator,
                error=exc,
                now=executed_at,
            )
        raise
