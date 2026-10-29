from datetime import datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class FrozenContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RecoveryActionType(StrEnum):
    REPLAY_VERIFIED_EVENT = "REPLAY_VERIFIED_EVENT"
    RESCHEDULE_OUTBOX_EVENT = "RESCHEDULE_OUTBOX_EVENT"
    RETAIN_QUARANTINE = "RETAIN_QUARANTINE"
    REQUEST_MANUAL_REVIEW = "REQUEST_MANUAL_REVIEW"


class EligibilityStatus(StrEnum):
    ELIGIBLE = "ELIGIBLE"
    INELIGIBLE = "INELIGIBLE"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class ExpectedRecoveryEffect(StrEnum):
    APPLY_EVENT = "APPLY_EVENT"
    RESCHEDULE_OUTBOX = "RESCHEDULE_OUTBOX"
    NO_OP_ALREADY_APPLIED = "NO_OP_ALREADY_APPLIED"
    NO_OP_ALREADY_PUBLISHED = "NO_OP_ALREADY_PUBLISHED"
    RETAIN_QUARANTINE = "RETAIN_QUARANTINE"
    REQUEST_MANUAL_REVIEW = "REQUEST_MANUAL_REVIEW"
    NONE = "NONE"


class RecoveryRejectionReason(StrEnum):
    POLICY_VERSION_MISMATCH = "POLICY_VERSION_MISMATCH"
    INVALID_PLAN_IDENTIFIER = "INVALID_PLAN_IDENTIFIER"
    UNSUPPORTED_ACTION = "UNSUPPORTED_ACTION"
    STALE_STATE = "STALE_STATE"
    MISSING_EVIDENCE = "MISSING_EVIDENCE"
    FOREIGN_EVIDENCE = "FOREIGN_EVIDENCE"
    EVIDENCE_TARGET_MISMATCH = "EVIDENCE_TARGET_MISMATCH"
    MISSING_AUTHORITATIVE_EVENT = "MISSING_AUTHORITATIVE_EVENT"
    MISSING_EVENT_HASH = "MISSING_EVENT_HASH"
    EVENT_HASH_MISMATCH = "EVENT_HASH_MISMATCH"
    EVENT_TARGET_MISMATCH = "EVENT_TARGET_MISMATCH"
    UNSUPPORTED_SCHEMA = "UNSUPPORTED_SCHEMA"
    MALFORMED_PAYLOAD = "MALFORMED_PAYLOAD"
    INVALID_BUSINESS_INVARIANT = "INVALID_BUSINESS_INVARIANT"
    POISON_EVENT = "POISON_EVENT"
    CONFLICTING_PROCESSED_EVENT = "CONFLICTING_PROCESSED_EVENT"
    CONFLICTING_PROJECTION = "CONFLICTING_PROJECTION"
    MISSING_OUTBOX_EVENT = "MISSING_OUTBOX_EVENT"
    OUTBOX_TARGET_MISMATCH = "OUTBOX_TARGET_MISMATCH"
    NOT_QUARANTINED = "NOT_QUARANTINED"
    CONCURRENT_VERIFICATION = "CONCURRENT_VERIFICATION"


class AuthoritativeEventSource(StrEnum):
    OUTBOX = "OUTBOX"
    QUARANTINE = "QUARANTINE"
    BROKER = "BROKER"


class RecoveryPlanSource(StrEnum):
    OPERATOR = "OPERATOR"
    INVESTIGATION = "INVESTIGATION"
    SYSTEM = "SYSTEM"


class RecoveryLifecycleState(StrEnum):
    PROPOSED = "PROPOSED"
    VERIFIED = "VERIFIED"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    APPROVED = "APPROVED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    NO_OP = "NO_OP"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


class RecoveryExecutionStatus(StrEnum):
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    NO_OP = "NO_OP"
    FAILED = "FAILED"


class KafkaCoordinate(FrozenContract):
    topic: str = Field(min_length=1, max_length=255)
    partition: int = Field(ge=0)
    offset: int = Field(ge=0)


class RecoveryTarget(FrozenContract):
    event_id: UUID | None = None
    aggregate_id: UUID | None = None
    kafka_coordinate: KafkaCoordinate | None = None
    outbox_event_id: UUID | None = None


class RecoveryPlan(FrozenContract):
    plan_id: UUID
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    policy_version: str = Field(min_length=1, max_length=100)
    action: RecoveryActionType
    incident_id: UUID
    target: RecoveryTarget
    evidence_ids: tuple[UUID, ...] = Field(default_factory=tuple, max_length=100)
    evidence_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_event_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    expected_state_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class RecoveryEvidence(FrozenContract):
    evidence_id: UUID
    incident_id: UUID
    event_id: UUID | None = None
    kafka_coordinate: KafkaCoordinate | None = None
    content_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class AuthoritativeEventData(FrozenContract):
    source: AuthoritativeEventSource
    raw_event: bytes
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_id: UUID | None = None
    kafka_coordinate: KafkaCoordinate | None = None
    outbox_event_id: UUID | None = None


class ProcessedEventSnapshot(FrozenContract):
    event_id: UUID
    event_type: str = Field(min_length=1, max_length=100)
    aggregate_id: UUID


class ProjectionSnapshot(FrozenContract):
    order_id: UUID
    source_event_id: UUID
    payload: dict[str, Any]


class OutboxEventSnapshot(FrozenContract):
    event_id: UUID
    event_type: str = Field(min_length=1, max_length=100)
    schema_version: int
    aggregate_id: UUID
    payload: dict[str, Any]
    published: bool
    attempts: int = Field(default=0, ge=0)
    next_attempt_at: datetime | None = None
    last_error_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class RecoveryStateSnapshot(FrozenContract):
    incident_id: UUID
    evidence: tuple[RecoveryEvidence, ...] = Field(default_factory=tuple, max_length=100)
    authoritative_event: AuthoritativeEventData | None = None
    processed_event: ProcessedEventSnapshot | None = None
    projection: ProjectionSnapshot | None = None
    outbox_event: OutboxEventSnapshot | None = None
    quarantined: bool = False


class EligibilityDecision(FrozenContract):
    status: EligibilityStatus
    policy_version: str
    plan_id: UUID
    action: RecoveryActionType
    state_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_effect: ExpectedRecoveryEffect = ExpectedRecoveryEffect.NONE
    reasons: tuple[RecoveryRejectionReason, ...] = Field(default_factory=tuple)
    validated_event_id: UUID | None = None
    validated_aggregate_id: UUID | None = None


class PersistedRecoveryPlan(FrozenContract):
    plan: RecoveryPlan
    source: RecoveryPlanSource
    created_by: str
    created_at: datetime
    current_state: RecoveryLifecycleState


class RecoveryVerificationView(FrozenContract):
    verification_id: UUID
    plan_id: UUID
    decision: EligibilityStatus
    reason_codes: tuple[RecoveryRejectionReason, ...]
    expected_effect: ExpectedRecoveryEffect
    verifier_version: str
    verified_at: datetime
    state_digest: str
    validated_event_id: UUID | None
    validated_aggregate_id: UUID | None
    simulated_write_set: tuple[str, ...] = ()
    postconditions: tuple[str, ...] = ()


class RecoveryEvidenceReference(FrozenContract):
    evidence_id: UUID
    incident_id: UUID
    event_id: UUID | None
    kafka_coordinate: KafkaCoordinate | None
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class RecoveryApprovalView(FrozenContract):
    approval_id: UUID
    plan_id: UUID
    verification_id: UUID
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    state_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    policy_version: str
    verifier_version: str
    operator_subject: str
    identity_provider: str
    approved_at: datetime
    expires_at: datetime


class RecoveryApprovalRequest(FrozenContract):
    idempotency_key: str = Field(min_length=8, max_length=255)


class RecoveryLifecycleEventView(FrozenContract):
    event_id: UUID
    plan_id: UUID
    sequence_number: int
    from_state: RecoveryLifecycleState | None
    to_state: RecoveryLifecycleState
    reason_code: str
    actor_type: RecoveryPlanSource
    actor_id: str
    occurred_at: datetime


class RecoveryPlanHistory(FrozenContract):
    plan: PersistedRecoveryPlan
    verifications: tuple[RecoveryVerificationView, ...]
    lifecycle_events: tuple[RecoveryLifecycleEventView, ...]


class RecoveryPlanReview(FrozenContract):
    plan: PersistedRecoveryPlan
    evidence: tuple[RecoveryEvidenceReference, ...]
    latest_verification: RecoveryVerificationView | None
    expected_effect: ExpectedRecoveryEffect | None
    declared_write_set: tuple[str, ...]
    postconditions: tuple[str, ...]
    approval: RecoveryApprovalView | None
    lifecycle_events: tuple[RecoveryLifecycleEventView, ...]


class RecoveryExpectedEffectsView(FrozenContract):
    expected_effect: ExpectedRecoveryEffect | None
    declared_write_set: tuple[str, ...]
    postconditions: tuple[str, ...]


class RecoveryDryRunResult(FrozenContract):
    plan_id: UUID
    decision: EligibilityDecision
    simulated_write_set: tuple[str, ...]
    postconditions: tuple[str, ...]
    business_state_rolled_back: Literal[True] = True
    kafka_publications: Literal[0] = 0
    persisted_verification: RecoveryVerificationView


class RecoveryExecutionEventView(FrozenContract):
    event_id: UUID
    execution_id: UUID
    plan_id: UUID
    sequence_number: int
    status: RecoveryExecutionStatus
    reason_code: str
    occurred_at: datetime
    pre_state_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    post_state_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    applied_write_set: tuple[str, ...]
    error_code: str | None


class RecoveryExecutionResult(FrozenContract):
    execution_id: UUID
    plan_id: UUID
    action: RecoveryActionType
    status: RecoveryExecutionStatus
    approval_id: UUID | None
    applied_write_set: tuple[str, ...]
    pre_state_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    post_state_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    events: tuple[RecoveryExecutionEventView, ...]
