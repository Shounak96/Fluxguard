import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from fluxguard.packages.common import recovery_policy
from fluxguard.packages.common.models import (
    RecoveryApproval,
    RecoveryLifecycleEvent,
    RecoveryPlanEntity,
    RecoveryVerification,
)
from fluxguard.packages.common.recoveries import _approval_view
from fluxguard.packages.common.recovery_policy import recovery_evidence_digest
from fluxguard.packages.common.recovery_verifier import load_authoritative_recovery_snapshot
from fluxguard.packages.common.time import utc_now
from fluxguard.packages.contracts.recovery import (
    EligibilityStatus,
    RecoveryActionType,
    RecoveryApprovalView,
    RecoveryLifecycleState,
    RecoveryPlan,
    RecoveryPlanSource,
)

DEFAULT_VERIFICATION_TTL = timedelta(minutes=15)
DEFAULT_APPROVAL_TTL = timedelta(minutes=15)
_APPROVAL_TRANSITIONS = frozenset(
    {
        (RecoveryLifecycleState.VERIFIED, RecoveryLifecycleState.AWAITING_APPROVAL),
        (RecoveryLifecycleState.AWAITING_APPROVAL, RecoveryLifecycleState.APPROVED),
        (RecoveryLifecycleState.VERIFIED, RecoveryLifecycleState.EXPIRED),
        (RecoveryLifecycleState.APPROVED, RecoveryLifecycleState.EXPIRED),
    }
)


@dataclass(frozen=True)
class AuthenticatedRecoveryOperator:
    subject: str
    identity_provider: str

    def __post_init__(self) -> None:
        if self.subject != self.subject.strip() or not self.subject or len(self.subject) > 100:
            raise ValueError("operator subject must contain 1 to 100 characters")
        if (
            self.identity_provider != self.identity_provider.strip()
            or not self.identity_provider
            or len(self.identity_provider) > 100
        ):
            raise ValueError("identity provider must contain 1 to 100 characters")


class RecoveryAuthorizer(Protocol):
    def authenticate(self) -> AuthenticatedRecoveryOperator: ...


class RecoveryApprovalError(RuntimeError):
    code = "RECOVERY_APPROVAL_ERROR"


class RecoveryAuthorizationError(RecoveryApprovalError):
    code = "RECOVERY_APPROVAL_DISABLED"


class RecoveryApprovalConflictError(RecoveryApprovalError):
    code = "RECOVERY_APPROVAL_CONFLICT"


class RecoveryApprovalExpiredError(RecoveryApprovalError):
    code = "RECOVERY_APPROVAL_EXPIRED"


class RecoveryApprovalStaleError(RecoveryApprovalError):
    code = "RECOVERY_PLAN_STALE"


class RecoveryApprovalTransitionError(RecoveryApprovalError):
    code = "INVALID_RECOVERY_TRANSITION"


class DisabledRecoveryAuthorizer:
    def authenticate(self) -> AuthenticatedRecoveryOperator:
        raise RecoveryAuthorizationError(
            "recovery approval is disabled because no trusted identity provider is configured"
        )


def _request_digest(plan_id: UUID, idempotency_key: str) -> str:
    key = idempotency_key.strip()
    if len(key) < 8 or len(key) > 255:
        raise ValueError("idempotency key must contain 8 to 255 characters")
    return hashlib.sha256(f"{plan_id}:{key}".encode()).hexdigest()


def _latest_lifecycle(session: Session, plan_id: UUID) -> RecoveryLifecycleEvent:
    event = session.scalar(
        select(RecoveryLifecycleEvent)
        .where(RecoveryLifecycleEvent.plan_id == plan_id)
        .order_by(RecoveryLifecycleEvent.sequence_number.desc())
        .limit(1)
        .with_for_update()
    )
    if event is None:
        raise LookupError("recovery plan has no lifecycle history")
    return event


def _append_lifecycle(
    session: Session,
    current: RecoveryLifecycleEvent,
    destination: RecoveryLifecycleState,
    reason: str,
    actor: AuthenticatedRecoveryOperator,
    occurred_at: datetime,
) -> RecoveryLifecycleEvent:
    transition = (RecoveryLifecycleState(current.to_state), destination)
    if transition not in _APPROVAL_TRANSITIONS:
        raise RecoveryApprovalTransitionError(
            f"approval transition {transition[0].value} -> {destination.value} is not allowed"
        )
    event = RecoveryLifecycleEvent(
        event_id=uuid4(),
        plan_id=current.plan_id,
        sequence_number=current.sequence_number + 1,
        from_state=current.to_state,
        to_state=destination.value,
        reason_code=reason,
        actor_type=RecoveryPlanSource.OPERATOR.value,
        actor_id=actor.subject,
        occurred_at=occurred_at,
    )
    session.add(event)
    session.flush()
    return event


def _expire_and_raise(
    session: Session,
    current: RecoveryLifecycleEvent,
    actor: AuthenticatedRecoveryOperator,
    now: datetime,
    reason: str,
    error: RecoveryApprovalError,
) -> RecoveryApprovalError:
    _append_lifecycle(session, current, RecoveryLifecycleState.EXPIRED, reason, actor, now)
    return error


def approve_recovery_plan(
    session: Session,
    plan_id: UUID,
    *,
    operator: AuthenticatedRecoveryOperator,
    idempotency_key: str,
    now: datetime | None = None,
    verification_ttl: timedelta = DEFAULT_VERIFICATION_TTL,
    approval_ttl: timedelta = DEFAULT_APPROVAL_TTL,
) -> RecoveryApprovalView:
    if session.in_transaction():
        raise RuntimeError("approval requires a fresh session")
    if verification_ttl <= timedelta(0) or approval_ttl <= timedelta(0):
        raise ValueError("verification and approval TTLs must be positive")
    approved_at = now or utc_now()
    request_digest = _request_digest(plan_id, idempotency_key)
    failure: RecoveryApprovalError | None = None
    result: RecoveryApprovalView | None = None
    with session.begin():
        entity = session.scalar(
            select(RecoveryPlanEntity)
            .where(RecoveryPlanEntity.plan_id == plan_id)
            .with_for_update()
        )
        if entity is None:
            raise LookupError("recovery plan not found")
        plan = RecoveryPlan.model_validate(entity.canonical_inputs)
        current = _latest_lifecycle(session, plan_id)
        existing = session.scalar(
            select(RecoveryApproval).where(RecoveryApproval.plan_id == plan_id)
        )
        if existing is not None:
            if approved_at >= existing.expires_at:
                if current.to_state == RecoveryLifecycleState.APPROVED.value:
                    _append_lifecycle(
                        session,
                        current,
                        RecoveryLifecycleState.EXPIRED,
                        "APPROVAL_EXPIRED",
                        operator,
                        approved_at,
                    )
                failure = RecoveryApprovalExpiredError("the recovery approval has expired")
            elif (
                existing.request_key_digest == request_digest
                and existing.operator_subject == operator.subject
                and existing.identity_provider == operator.identity_provider
            ):
                result = _approval_view(existing)
            else:
                failure = RecoveryApprovalConflictError(
                    "the recovery plan already has a different approval"
                )
        elif current.to_state != RecoveryLifecycleState.VERIFIED.value:
            failure = RecoveryApprovalTransitionError(
                f"cannot approve a plan in lifecycle state {current.to_state}"
            )
        elif plan.action not in {
            RecoveryActionType.REPLAY_VERIFIED_EVENT,
            RecoveryActionType.RESCHEDULE_OUTBOX_EVENT,
        }:
            failure = RecoveryApprovalTransitionError(
                "this non-mutating recovery action does not require approval"
            )
        else:
            verification = session.scalar(
                select(RecoveryVerification).where(RecoveryVerification.plan_id == plan_id)
            )
            if verification is None or verification.decision != EligibilityStatus.ELIGIBLE.value:
                failure = RecoveryApprovalTransitionError(
                    "an eligible verification is required before approval"
                )
            elif approved_at - verification.verified_at > verification_ttl:
                failure = _expire_and_raise(
                    session,
                    current,
                    operator,
                    approved_at,
                    "VERIFICATION_EXPIRED",
                    RecoveryApprovalExpiredError("the recovery verification has expired"),
                )
            elif (
                plan.policy_version != recovery_policy.RECOVERY_POLICY_VERSION
                or verification.verifier_version != recovery_policy.RECOVERY_POLICY_VERSION
            ):
                failure = _expire_and_raise(
                    session,
                    current,
                    operator,
                    approved_at,
                    "VERSION_MISMATCH",
                    RecoveryApprovalStaleError("policy or verifier version no longer matches"),
                )
            else:
                snapshot = load_authoritative_recovery_snapshot(session, plan)
                state_digest = recovery_policy.recovery_state_digest(snapshot)
                evidence_digest = recovery_evidence_digest(snapshot.evidence)
                if (
                    state_digest != verification.state_digest
                    or state_digest != plan.expected_state_digest
                    or evidence_digest != plan.evidence_digest
                ):
                    failure = _expire_and_raise(
                        session,
                        current,
                        operator,
                        approved_at,
                        "STALE_RECOVERY_STATE",
                        RecoveryApprovalStaleError(
                            "authoritative evidence or state changed after verification"
                        ),
                    )
                else:
                    awaiting = _append_lifecycle(
                        session,
                        current,
                        RecoveryLifecycleState.AWAITING_APPROVAL,
                        "APPROVAL_REQUESTED",
                        operator,
                        approved_at,
                    )
                    approval = RecoveryApproval(
                        approval_id=uuid4(),
                        plan_id=plan_id,
                        verification_id=verification.verification_id,
                        plan_hash=plan.plan_hash,
                        evidence_digest=plan.evidence_digest,
                        state_digest=verification.state_digest,
                        policy_version=plan.policy_version,
                        verifier_version=verification.verifier_version,
                        operator_subject=operator.subject,
                        identity_provider=operator.identity_provider,
                        request_key_digest=request_digest,
                        approved_at=approved_at,
                        expires_at=approved_at + approval_ttl,
                    )
                    session.add(approval)
                    _append_lifecycle(
                        session,
                        awaiting,
                        RecoveryLifecycleState.APPROVED,
                        "APPROVAL_GRANTED",
                        operator,
                        approved_at,
                    )
                    session.flush()
                    result = _approval_view(approval)
    if failure is not None:
        raise failure
    if result is None:
        raise RecoveryApprovalError("approval did not produce a result")
    return result
