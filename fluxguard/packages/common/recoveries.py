import hashlib
import json
import re
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from fluxguard.packages.common.models import (
    Incident,
    IncidentEvidence,
    RecoveryApproval,
    RecoveryLifecycleEvent,
    RecoveryPlanEntity,
    RecoveryPlanEvidenceLink,
    RecoveryVerification,
)
from fluxguard.packages.common.recovery_policy import (
    recovery_evidence_digest,
    recovery_plan_identity_is_valid,
)
from fluxguard.packages.common.time import utc_now
from fluxguard.packages.contracts.recovery import (
    EligibilityDecision,
    EligibilityStatus,
    KafkaCoordinate,
    PersistedRecoveryPlan,
    RecoveryApprovalView,
    RecoveryEvidence,
    RecoveryEvidenceReference,
    RecoveryLifecycleEventView,
    RecoveryLifecycleState,
    RecoveryPlan,
    RecoveryPlanHistory,
    RecoveryPlanReview,
    RecoveryPlanSource,
    RecoveryRejectionReason,
    RecoveryVerificationView,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class RecoveryPersistenceError(RuntimeError):
    pass


class RecoveryPlanConflictError(RecoveryPersistenceError):
    pass


class RecoveryEvidenceOwnershipError(RecoveryPersistenceError):
    pass


class InvalidRecoveryTransitionError(RecoveryPersistenceError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def recovery_evidence_from_row(evidence: IncidentEvidence) -> RecoveryEvidence:
    raw_hash = evidence.payload.get("raw_sha256") if isinstance(evidence.payload, dict) else None
    content_sha256 = raw_hash if isinstance(raw_hash, str) and _SHA256.fullmatch(raw_hash) else None
    coordinate = None
    if (
        evidence.kafka_topic is not None
        and evidence.kafka_partition is not None
        and evidence.kafka_offset is not None
    ):
        coordinate = KafkaCoordinate(
            topic=evidence.kafka_topic,
            partition=evidence.kafka_partition,
            offset=evidence.kafka_offset,
        )
    return RecoveryEvidence(
        evidence_id=evidence.evidence_id,
        incident_id=evidence.incident_id,
        event_id=evidence.event_id,
        kafka_coordinate=coordinate,
        content_sha256=content_sha256,
    )


def _evidence_row_digest(evidence: IncidentEvidence) -> str:
    return _sha256(
        {
            "evidence_id": evidence.evidence_id,
            "incident_id": evidence.incident_id,
            "evidence_type": evidence.evidence_type,
            "source": evidence.source,
            "event_id": evidence.event_id,
            "kafka_topic": evidence.kafka_topic,
            "kafka_partition": evidence.kafka_partition,
            "kafka_offset": evidence.kafka_offset,
            "observed_at": evidence.observed_at.isoformat(),
            "payload": evidence.payload,
            "dedup_key": evidence.dedup_key,
        }
    )


def _plan_payload(plan: RecoveryPlan) -> dict[str, Any]:
    return plan.model_dump(mode="json")


def _same_plan(entity: RecoveryPlanEntity, plan: RecoveryPlan) -> bool:
    return entity.canonical_inputs == _plan_payload(plan)


def _insert_plan(session: Session, values: dict[str, Any]) -> UUID | None:
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        statement = postgresql_insert(RecoveryPlanEntity).values(**values)
    elif dialect == "sqlite":
        statement = sqlite_insert(RecoveryPlanEntity).values(**values)
    else:
        raise RecoveryPersistenceError(f"unsupported database dialect: {dialect}")
    statement = statement.on_conflict_do_nothing(
        index_elements=[RecoveryPlanEntity.plan_id]
    ).returning(RecoveryPlanEntity.plan_id)
    return session.execute(statement).scalar_one_or_none()


def _load_evidence(
    session: Session, plan: RecoveryPlan
) -> tuple[list[IncidentEvidence], tuple[RecoveryEvidence, ...]]:
    if not plan.evidence_ids:
        return [], ()
    rows = session.scalars(
        select(IncidentEvidence).where(IncidentEvidence.evidence_id.in_(plan.evidence_ids))
    ).all()
    by_id = {item.evidence_id: item for item in rows}
    if set(by_id) != set(plan.evidence_ids):
        raise RecoveryEvidenceOwnershipError("one or more cited evidence records do not exist")
    ordered = [by_id[evidence_id] for evidence_id in plan.evidence_ids]
    if any(item.incident_id != plan.incident_id for item in ordered):
        raise RecoveryEvidenceOwnershipError("cited evidence does not belong to the plan incident")
    contracts = tuple(recovery_evidence_from_row(item) for item in ordered)
    if recovery_evidence_digest(contracts) != plan.evidence_digest:
        raise RecoveryEvidenceOwnershipError("cited evidence digest does not match the plan")
    return ordered, contracts


def create_recovery_plan(
    session: Session,
    plan: RecoveryPlan,
    *,
    source: RecoveryPlanSource,
    created_by: str,
) -> tuple[PersistedRecoveryPlan, bool]:
    actor = created_by.strip()
    if not actor or len(actor) > 100:
        raise ValueError("created_by must contain 1 to 100 characters")
    existing = session.get(RecoveryPlanEntity, plan.plan_id)
    if existing is not None:
        if not _same_plan(existing, plan):
            raise RecoveryPlanConflictError("plan ID already exists with different content")
        return get_recovery_plan(session, plan.plan_id), False
    if not recovery_plan_identity_is_valid(plan):
        raise RecoveryPlanConflictError("plan identity does not match canonical content")
    if session.get(Incident, plan.incident_id) is None:
        raise LookupError("incident not found")
    evidence_rows, _ = _load_evidence(session, plan)
    now = utc_now()
    values = {
        "plan_id": plan.plan_id,
        "plan_hash": plan.plan_hash,
        "incident_id": plan.incident_id,
        "action_type": plan.action.value,
        "target": plan.target.model_dump(mode="json"),
        "canonical_inputs": _plan_payload(plan),
        "evidence_digest": plan.evidence_digest,
        "expected_event_sha256": plan.expected_event_sha256,
        "state_digest": plan.expected_state_digest,
        "policy_version": plan.policy_version,
        "source": source.value,
        "created_by": actor,
        "created_at": now,
    }
    inserted = _insert_plan(session, values)
    if inserted is None:
        existing = session.get(RecoveryPlanEntity, plan.plan_id, populate_existing=True)
        if existing is None or not _same_plan(existing, plan):
            raise RecoveryPlanConflictError("plan ID already exists with different content")
        return get_recovery_plan(session, plan.plan_id), False
    session.add_all(
        [
            RecoveryPlanEvidenceLink(
                plan_id=plan.plan_id,
                evidence_id=item.evidence_id,
                incident_id=plan.incident_id,
                content_digest=_evidence_row_digest(item),
            )
            for item in evidence_rows
        ]
    )
    session.add(
        RecoveryLifecycleEvent(
            event_id=uuid4(),
            plan_id=plan.plan_id,
            sequence_number=1,
            from_state=None,
            to_state=RecoveryLifecycleState.PROPOSED.value,
            reason_code="PLAN_CREATED",
            actor_type=source.value,
            actor_id=actor,
            occurred_at=now,
        )
    )
    session.flush()
    return get_recovery_plan(session, plan.plan_id), True


def _current_lifecycle_event(session: Session, plan_id: UUID) -> RecoveryLifecycleEvent:
    event = session.scalar(
        select(RecoveryLifecycleEvent)
        .where(RecoveryLifecycleEvent.plan_id == plan_id)
        .order_by(RecoveryLifecycleEvent.sequence_number.desc())
        .limit(1)
    )
    if event is None:
        raise RecoveryPersistenceError("recovery plan has no lifecycle history")
    return event


def record_recovery_verification(
    session: Session,
    plan_id: UUID,
    decision: EligibilityDecision,
    *,
    verifier_version: str,
    simulated_write_set: tuple[str, ...] = (),
    postconditions: tuple[str, ...] = (),
) -> RecoveryVerificationView:
    verifier = verifier_version.strip()
    if not verifier or len(verifier) > 100:
        raise ValueError("verifier_version must contain 1 to 100 characters")
    entity = session.scalar(
        select(RecoveryPlanEntity).where(RecoveryPlanEntity.plan_id == plan_id).with_for_update()
    )
    if entity is None:
        raise LookupError("recovery plan not found")
    plan = RecoveryPlan.model_validate(entity.canonical_inputs)
    policy_mismatch_rejection = (
        decision.status == EligibilityStatus.INELIGIBLE
        and RecoveryRejectionReason.POLICY_VERSION_MISMATCH in decision.reasons
    )
    if (
        decision.plan_id != plan_id
        or decision.action != plan.action
        or (decision.policy_version != plan.policy_version and not policy_mismatch_rejection)
    ):
        raise RecoveryPlanConflictError("verification decision does not match the plan")
    if entity.state_digest != plan.expected_state_digest:
        raise RecoveryPlanConflictError("persisted state digest does not match the plan")
    if (
        decision.status == EligibilityStatus.ELIGIBLE
        and decision.state_digest != entity.state_digest
    ):
        raise RecoveryPlanConflictError("eligible verification used a different state digest")
    current = _current_lifecycle_event(session, plan_id)
    if current.to_state != RecoveryLifecycleState.PROPOSED.value:
        raise InvalidRecoveryTransitionError(
            f"cannot verify a plan in lifecycle state {current.to_state}"
        )
    now = utc_now()
    verification = RecoveryVerification(
        verification_id=uuid4(),
        plan_id=plan_id,
        decision=decision.status.value,
        reason_codes=[item.value for item in decision.reasons],
        expected_effect=decision.expected_effect.value,
        verifier_version=verifier,
        verified_at=now,
        state_digest=decision.state_digest,
        validated_event_id=decision.validated_event_id,
        validated_aggregate_id=decision.validated_aggregate_id,
        simulated_write_set=list(simulated_write_set),
        postconditions=list(postconditions),
    )
    destination = (
        RecoveryLifecycleState.VERIFIED
        if decision.status == EligibilityStatus.ELIGIBLE
        else RecoveryLifecycleState.REJECTED
    )
    reason_code = "VERIFICATION_ELIGIBLE" if not decision.reasons else decision.reasons[0].value
    lifecycle = RecoveryLifecycleEvent(
        event_id=uuid4(),
        plan_id=plan_id,
        sequence_number=current.sequence_number + 1,
        from_state=current.to_state,
        to_state=destination.value,
        reason_code=reason_code,
        actor_type=RecoveryPlanSource.SYSTEM.value,
        actor_id=verifier,
        occurred_at=now,
    )
    session.add_all([verification, lifecycle])
    session.flush()
    return _verification_view(verification)


def _lifecycle_view(event: RecoveryLifecycleEvent) -> RecoveryLifecycleEventView:
    return RecoveryLifecycleEventView(
        event_id=event.event_id,
        plan_id=event.plan_id,
        sequence_number=event.sequence_number,
        from_state=(RecoveryLifecycleState(event.from_state) if event.from_state else None),
        to_state=RecoveryLifecycleState(event.to_state),
        reason_code=event.reason_code,
        actor_type=RecoveryPlanSource(event.actor_type),
        actor_id=event.actor_id,
        occurred_at=event.occurred_at,
    )


def _verification_view(verification: RecoveryVerification) -> RecoveryVerificationView:
    return RecoveryVerificationView(
        verification_id=verification.verification_id,
        plan_id=verification.plan_id,
        decision=EligibilityStatus(verification.decision),
        reason_codes=tuple(verification.reason_codes),
        expected_effect=verification.expected_effect,
        verifier_version=verification.verifier_version,
        verified_at=verification.verified_at,
        state_digest=verification.state_digest,
        validated_event_id=verification.validated_event_id,
        validated_aggregate_id=verification.validated_aggregate_id,
        simulated_write_set=tuple(verification.simulated_write_set),
        postconditions=tuple(verification.postconditions),
    )


def _approval_view(approval: RecoveryApproval) -> RecoveryApprovalView:
    return RecoveryApprovalView(
        approval_id=approval.approval_id,
        plan_id=approval.plan_id,
        verification_id=approval.verification_id,
        plan_hash=approval.plan_hash,
        evidence_digest=approval.evidence_digest,
        state_digest=approval.state_digest,
        policy_version=approval.policy_version,
        verifier_version=approval.verifier_version,
        operator_subject=approval.operator_subject,
        identity_provider=approval.identity_provider,
        approved_at=approval.approved_at,
        expires_at=approval.expires_at,
    )


def get_recovery_plan(session: Session, plan_id: UUID) -> PersistedRecoveryPlan:
    entity = session.get(RecoveryPlanEntity, plan_id)
    if entity is None:
        raise LookupError("recovery plan not found")
    current = _current_lifecycle_event(session, plan_id)
    return PersistedRecoveryPlan(
        plan=RecoveryPlan.model_validate(entity.canonical_inputs),
        source=RecoveryPlanSource(entity.source),
        created_by=entity.created_by,
        created_at=entity.created_at,
        current_state=RecoveryLifecycleState(current.to_state),
    )


def get_recovery_plan_history(session: Session, plan_id: UUID) -> RecoveryPlanHistory:
    plan = get_recovery_plan(session, plan_id)
    verifications = session.scalars(
        select(RecoveryVerification)
        .where(RecoveryVerification.plan_id == plan_id)
        .order_by(RecoveryVerification.verified_at, RecoveryVerification.verification_id)
    ).all()
    lifecycle = session.scalars(
        select(RecoveryLifecycleEvent)
        .where(RecoveryLifecycleEvent.plan_id == plan_id)
        .order_by(RecoveryLifecycleEvent.sequence_number)
    ).all()
    return RecoveryPlanHistory(
        plan=plan,
        verifications=tuple(_verification_view(item) for item in verifications),
        lifecycle_events=tuple(_lifecycle_view(item) for item in lifecycle),
    )


def get_recovery_plan_review(session: Session, plan_id: UUID) -> RecoveryPlanReview:
    history = get_recovery_plan_history(session, plan_id)
    links = session.scalars(
        select(RecoveryPlanEvidenceLink)
        .where(RecoveryPlanEvidenceLink.plan_id == plan_id)
        .order_by(RecoveryPlanEvidenceLink.evidence_id)
    ).all()
    rows = {
        row.evidence_id: row
        for row in session.scalars(
            select(IncidentEvidence).where(
                IncidentEvidence.evidence_id.in_([link.evidence_id for link in links])
            )
        ).all()
    }
    evidence = []
    for link in links:
        row = rows.get(link.evidence_id)
        if row is None or row.incident_id != history.plan.plan.incident_id:
            raise RecoveryEvidenceOwnershipError("persisted evidence ownership is invalid")
        contract = recovery_evidence_from_row(row)
        evidence.append(
            RecoveryEvidenceReference(
                evidence_id=link.evidence_id,
                incident_id=link.incident_id,
                event_id=contract.event_id,
                kafka_coordinate=contract.kafka_coordinate,
                content_digest=link.content_digest,
            )
        )
    latest = history.verifications[-1] if history.verifications else None
    approval = session.scalar(select(RecoveryApproval).where(RecoveryApproval.plan_id == plan_id))
    return RecoveryPlanReview(
        plan=history.plan,
        evidence=tuple(evidence),
        latest_verification=latest,
        expected_effect=latest.expected_effect if latest else None,
        declared_write_set=latest.simulated_write_set if latest else (),
        postconditions=latest.postconditions if latest else (),
        approval=_approval_view(approval) if approval else None,
        lifecycle_events=history.lifecycle_events,
    )
