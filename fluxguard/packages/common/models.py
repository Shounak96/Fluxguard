from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fluxguard.packages.common.db import Base
from fluxguard.packages.common.time import utc_now


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint("total >= 0", name="ck_orders_total_nonnegative"),
        CheckConstraint("length(currency) = 3", name="ck_orders_currency_length"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(100), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    total: Mapped[Decimal] = mapped_column(Numeric(22, 2), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    items: Mapped[list["OrderItem"]] = relationship(
        back_populates="order", cascade="all, delete-orphan", lazy="selectin"
    )


class OrderItem(Base):
    __tablename__ = "order_items"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_order_items_quantity_positive"),
        CheckConstraint("unit_price >= 0", name="ck_order_items_price_nonnegative"),
        CheckConstraint("line_total >= 0", name="ck_order_items_total_nonnegative"),
        UniqueConstraint("order_id", "position"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[UUID] = mapped_column(
        ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    product_id: Mapped[str] = mapped_column(String(100), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    line_total: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    order: Mapped[Order] = relationship(back_populates="items")


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint("attempts >= 0", name="ck_outbox_attempts_nonnegative"),
        Index("ix_outbox_pending", "published_at", "next_attempt_at", "occurred_at"),
    )

    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(nullable=False, index=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    __table_args__ = (
        CheckConstraint("delivery_count >= 1", name="ck_processed_delivery_count_positive"),
    )

    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(nullable=False, index=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    delivery_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class OrderProjection(Base):
    __tablename__ = "order_projections"
    __table_args__ = (
        CheckConstraint("total >= 0", name="ck_projection_total_nonnegative"),
        CheckConstraint("item_count > 0", name="ck_projection_item_count_positive"),
    )

    order_id: Mapped[UUID] = mapped_column(primary_key=True)
    source_event_id: Mapped[UUID] = mapped_column(nullable=False, unique=True)
    customer_id: Mapped[str] = mapped_column(String(100), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    total: Mapped[Decimal] = mapped_column(Numeric(22, 2), nullable=False)
    item_count: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    projected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class QuarantinedEvent(Base):
    __tablename__ = "quarantined_events"
    __table_args__ = (
        UniqueConstraint("topic", "partition", "offset", name="uq_quarantine_record"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    partition: Mapped[int] = mapped_column(Integer, nullable=False)
    offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    message_key: Mapped[str | None] = mapped_column(Text)
    raw_payload: Mapped[str | None] = mapped_column(Text)
    error_type: Mapped[str] = mapped_column(String(100), nullable=False)
    error_detail: Mapped[str] = mapped_column(Text, nullable=False)
    quarantined_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class IncidentStatus(StrEnum):
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"


class IncidentSeverity(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class InvestigationStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    UNVERIFIED = "UNVERIFIED"
    FAILED = "FAILED"


class Incident(Base):
    __tablename__ = "incidents"
    __table_args__ = (
        CheckConstraint("occurrence_count >= 1", name="ck_incident_occurrence_positive"),
        CheckConstraint(
            "status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')",
            name="ck_incident_status",
        ),
        CheckConstraint(
            "severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')",
            name="ck_incident_severity",
        ),
        Index("ix_incidents_status_detected", "status", "detected_at"),
        Index("ix_incidents_type_severity", "incident_type", "severity"),
    )

    incident_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    incident_type: Mapped[str] = mapped_column(String(100), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=IncidentStatus.OPEN.value
    )
    source_component: Mapped[str] = mapped_column(String(100), nullable=False)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    occurrence_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    error_code: Mapped[str] = mapped_column(String(100), nullable=False)
    summary: Mapped[str] = mapped_column(String(500), nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    details: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, nullable=False)
    evidence: Mapped[list["IncidentEvidence"]] = relationship(
        back_populates="incident", cascade="all, delete-orphan"
    )


class IncidentEvidence(Base):
    __tablename__ = "incident_evidence"
    __table_args__ = (
        UniqueConstraint("evidence_id", "incident_id", name="uq_incident_evidence_id_incident"),
        Index("ix_incident_evidence_incident_observed", "incident_id", "observed_at"),
    )

    evidence_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    incident_id: Mapped[UUID] = mapped_column(
        ForeignKey("incidents.incident_id", ondelete="CASCADE"), nullable=False, index=True
    )
    evidence_type: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    event_id: Mapped[UUID | None] = mapped_column(index=True)
    kafka_topic: Mapped[str | None] = mapped_column(String(255))
    kafka_partition: Mapped[int | None] = mapped_column(Integer)
    kafka_offset: Mapped[int | None] = mapped_column(BigInteger)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    dedup_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    incident: Mapped[Incident] = relationship(back_populates="evidence")


class ConsumerRetryState(Base):
    __tablename__ = "consumer_retry_state"
    __table_args__ = (
        UniqueConstraint("topic", "partition", "offset", name="uq_consumer_retry_record"),
        CheckConstraint("attempt_count >= 1", name="ck_consumer_retry_attempt_positive"),
    )

    retry_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    partition: Mapped[int] = mapped_column(Integer, nullable=False)
    offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_id: Mapped[UUID | None]
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    first_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_error: Mapped[str] = mapped_column(String(1000), nullable=False)


class InvestigationRun(Base):
    __tablename__ = "investigation_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'INSUFFICIENT_EVIDENCE', "
            "'UNVERIFIED', 'FAILED')",
            name="ck_investigation_status",
        ),
        CheckConstraint("llm_call_count >= 0", name="ck_investigation_call_count_nonnegative"),
        Index("ix_investigation_incident_created", "incident_id", "created_at"),
    )

    investigation_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    incident_id: Mapped[UUID] = mapped_column(
        ForeignKey("incidents.incident_id", ondelete="CASCADE"), nullable=False, index=True
    )
    model_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default=InvestigationStatus.PENDING.value
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    diagnosis: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    tool_call_history: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list
    )
    prompt_tokens: Mapped[int | None] = mapped_column(Integer)
    completion_tokens: Mapped[int | None] = mapped_column(Integer)
    total_tokens: Mapped[int | None] = mapped_column(Integer)
    estimated_cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(12, 6))
    llm_call_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_category: Mapped[str | None] = mapped_column(String(100))
    error_message: Mapped[str | None] = mapped_column(String(500))


class RecoveryPlanEntity(Base):
    __tablename__ = "recovery_plans"
    __table_args__ = (
        CheckConstraint(
            "action_type IN ('REPLAY_VERIFIED_EVENT', 'RESCHEDULE_OUTBOX_EVENT', "
            "'RETAIN_QUARANTINE', 'REQUEST_MANUAL_REVIEW')",
            name="ck_recovery_plan_action",
        ),
        CheckConstraint(
            "source IN ('OPERATOR', 'INVESTIGATION', 'SYSTEM')",
            name="ck_recovery_plan_source",
        ),
        UniqueConstraint("plan_id", "incident_id", name="uq_recovery_plan_id_incident"),
    )

    plan_id: Mapped[UUID] = mapped_column(primary_key=True)
    plan_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    incident_id: Mapped[UUID] = mapped_column(
        ForeignKey("incidents.incident_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    action_type: Mapped[str] = mapped_column(String(50), nullable=False)
    target: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    canonical_inputs: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    evidence_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    expected_event_sha256: Mapped[str | None] = mapped_column(String(64))
    state_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(30), nullable=False)
    created_by: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class RecoveryPlanEvidenceLink(Base):
    __tablename__ = "recovery_plan_evidence"
    __table_args__ = (
        ForeignKeyConstraint(
            ["plan_id", "incident_id"],
            ["recovery_plans.plan_id", "recovery_plans.incident_id"],
            ondelete="RESTRICT",
            name="fk_recovery_evidence_plan_incident",
        ),
        ForeignKeyConstraint(
            ["evidence_id", "incident_id"],
            ["incident_evidence.evidence_id", "incident_evidence.incident_id"],
            ondelete="RESTRICT",
            name="fk_recovery_evidence_incident_owner",
        ),
    )

    plan_id: Mapped[UUID] = mapped_column(primary_key=True)
    evidence_id: Mapped[UUID] = mapped_column(primary_key=True)
    incident_id: Mapped[UUID] = mapped_column(nullable=False)
    content_digest: Mapped[str] = mapped_column(String(64), nullable=False)


class RecoveryVerification(Base):
    __tablename__ = "recovery_verifications"
    __table_args__ = (
        CheckConstraint(
            "decision IN ('ELIGIBLE', 'INELIGIBLE', 'INSUFFICIENT_EVIDENCE')",
            name="ck_recovery_verification_decision",
        ),
        UniqueConstraint("plan_id", name="uq_recovery_verification_plan"),
    )

    verification_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    plan_id: Mapped[UUID] = mapped_column(
        ForeignKey("recovery_plans.plan_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    decision: Mapped[str] = mapped_column(String(30), nullable=False)
    reason_codes: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    expected_effect: Mapped[str] = mapped_column(String(50), nullable=False)
    verifier_version: Mapped[str] = mapped_column(String(100), nullable=False)
    verified_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    state_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    validated_event_id: Mapped[UUID | None]
    validated_aggregate_id: Mapped[UUID | None]
    simulated_write_set: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    postconditions: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)


class RecoveryApproval(Base):
    __tablename__ = "recovery_approvals"
    __table_args__ = (
        CheckConstraint("expires_at > approved_at", name="ck_recovery_approval_expiry"),
        UniqueConstraint("plan_id", name="uq_recovery_approval_plan"),
        UniqueConstraint("request_key_digest", name="uq_recovery_approval_request_key"),
    )

    approval_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    plan_id: Mapped[UUID] = mapped_column(
        ForeignKey("recovery_plans.plan_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    verification_id: Mapped[UUID] = mapped_column(
        ForeignKey("recovery_verifications.verification_id", ondelete="RESTRICT"), nullable=False
    )
    plan_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    state_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(100), nullable=False)
    verifier_version: Mapped[str] = mapped_column(String(100), nullable=False)
    operator_subject: Mapped[str] = mapped_column(String(255), nullable=False)
    identity_provider: Mapped[str] = mapped_column(String(100), nullable=False)
    request_key_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    approved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RecoveryLifecycleEvent(Base):
    __tablename__ = "recovery_lifecycle_events"
    __table_args__ = (
        CheckConstraint("sequence_number >= 1", name="ck_recovery_lifecycle_sequence"),
        CheckConstraint(
            "from_state IS NULL OR from_state IN ('PROPOSED', 'VERIFIED', "
            "'AWAITING_APPROVAL', 'APPROVED', 'EXECUTING', 'SUCCEEDED', 'NO_OP', "
            "'FAILED', 'EXPIRED', 'REJECTED')",
            name="ck_recovery_lifecycle_from_state",
        ),
        CheckConstraint(
            "to_state IN ('PROPOSED', 'VERIFIED', 'AWAITING_APPROVAL', 'APPROVED', "
            "'EXECUTING', 'SUCCEEDED', 'NO_OP', 'FAILED', 'EXPIRED', 'REJECTED')",
            name="ck_recovery_lifecycle_to_state",
        ),
        CheckConstraint(
            "actor_type IN ('OPERATOR', 'INVESTIGATION', 'SYSTEM')",
            name="ck_recovery_lifecycle_actor",
        ),
        UniqueConstraint("plan_id", "sequence_number", name="uq_recovery_lifecycle_plan_sequence"),
    )

    event_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    plan_id: Mapped[UUID] = mapped_column(
        ForeignKey("recovery_plans.plan_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    from_state: Mapped[str | None] = mapped_column(String(30))
    to_state: Mapped[str] = mapped_column(String(30), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(100), nullable=False)
    actor_type: Mapped[str] = mapped_column(String(30), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(100), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class RecoveryExecutionRequest(Base):
    __tablename__ = "recovery_execution_requests"
    __table_args__ = (
        CheckConstraint(
            "action_type IN ('REPLAY_VERIFIED_EVENT', 'RESCHEDULE_OUTBOX_EVENT', "
            "'RETAIN_QUARANTINE', 'REQUEST_MANUAL_REVIEW')",
            name="ck_recovery_execution_action",
        ),
        UniqueConstraint("execution_id", "plan_id", name="uq_recovery_execution_request_plan"),
    )

    execution_id: Mapped[UUID] = mapped_column(primary_key=True)
    plan_id: Mapped[UUID] = mapped_column(
        ForeignKey("recovery_plans.plan_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    approval_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("recovery_approvals.approval_id", ondelete="RESTRICT")
    )
    request_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    plan_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    action_type: Mapped[str] = mapped_column(String(50), nullable=False)
    actor_subject: Mapped[str] = mapped_column(String(100), nullable=False)
    identity_provider: Mapped[str] = mapped_column(String(100), nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RecoveryExecutionEvent(Base):
    __tablename__ = "recovery_execution_events"
    __table_args__ = (
        CheckConstraint("sequence_number >= 1", name="ck_recovery_execution_event_sequence"),
        CheckConstraint(
            "status IN ('STARTED', 'SUCCEEDED', 'NO_OP', 'FAILED')",
            name="ck_recovery_execution_event_status",
        ),
        ForeignKeyConstraint(
            ["execution_id", "plan_id"],
            ["recovery_execution_requests.execution_id", "recovery_execution_requests.plan_id"],
            ondelete="RESTRICT",
            name="fk_recovery_execution_event_request",
        ),
        UniqueConstraint(
            "execution_id", "sequence_number", name="uq_recovery_execution_event_sequence"
        ),
    )

    event_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    execution_id: Mapped[UUID] = mapped_column(nullable=False, index=True)
    plan_id: Mapped[UUID] = mapped_column(nullable=False, index=True)
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(100), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    pre_state_digest: Mapped[str | None] = mapped_column(String(64))
    post_state_digest: Mapped[str | None] = mapped_column(String(64))
    applied_write_set: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    error_code: Mapped[str | None] = mapped_column(String(100))
    error_message: Mapped[str | None] = mapped_column(String(500))
