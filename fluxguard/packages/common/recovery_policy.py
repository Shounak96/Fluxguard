import hashlib
import json
from typing import Any
from uuid import UUID, uuid5

from pydantic import ValidationError

from fluxguard.apps.worker.consumer import PermanentEventError, parse_event
from fluxguard.packages.contracts.events import ORDER_CREATED_EVENT_TYPE, OrderCreatedEvent
from fluxguard.packages.contracts.recovery import (
    AuthoritativeEventSource,
    EligibilityDecision,
    EligibilityStatus,
    ExpectedRecoveryEffect,
    RecoveryActionType,
    RecoveryEvidence,
    RecoveryPlan,
    RecoveryRejectionReason,
    RecoveryStateSnapshot,
    RecoveryTarget,
)

RECOVERY_POLICY_VERSION = "fluxguard.recovery.v1"
_PLAN_NAMESPACE = UUID("87ebf9af-3602-52de-86c7-5cc1ca447f3e")
_MUTATING_ACTIONS = frozenset(
    {
        RecoveryActionType.REPLAY_VERIFIED_EVENT,
        RecoveryActionType.RESCHEDULE_OUTBOX_EVENT,
    }
)


def _json_default(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return hashlib.sha256(value).hexdigest()
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _sha256(value: bytes | str) -> str:
    data = value.encode() if isinstance(value, str) else value
    return hashlib.sha256(data).hexdigest()


def recovery_state_digest(state: RecoveryStateSnapshot) -> str:
    payload = state.model_dump(mode="json")
    payload["evidence"] = sorted(payload["evidence"], key=lambda item: item["evidence_id"])
    authoritative = payload.get("authoritative_event")
    if authoritative is not None:
        authoritative["raw_event"] = _sha256(state.authoritative_event.raw_event)
    return _sha256(_canonical_json(payload))


def recovery_evidence_digest(evidence: tuple[RecoveryEvidence, ...]) -> str:
    payload = [
        item.model_dump(mode="json")
        for item in sorted(evidence, key=lambda item: str(item.evidence_id))
    ]
    return _sha256(_canonical_json(payload))


def _plan_material(
    *,
    policy_version: str,
    action: RecoveryActionType,
    incident_id: UUID,
    target: RecoveryTarget,
    evidence_ids: tuple[UUID, ...],
    evidence_digest: str,
    expected_event_sha256: str | None,
    expected_state_digest: str,
) -> dict[str, Any]:
    return {
        "policy_version": policy_version,
        "action": action.value,
        "incident_id": str(incident_id),
        "target": target.model_dump(mode="json"),
        "evidence_ids": [str(value) for value in evidence_ids],
        "evidence_digest": evidence_digest,
        "expected_event_sha256": expected_event_sha256,
        "expected_state_digest": expected_state_digest,
    }


def build_recovery_plan(
    *,
    action: RecoveryActionType,
    incident_id: UUID,
    target: RecoveryTarget,
    evidence_ids: list[UUID] | tuple[UUID, ...],
    state: RecoveryStateSnapshot,
    expected_event_sha256: str | None = None,
) -> RecoveryPlan:
    ordered_evidence = tuple(sorted(set(evidence_ids), key=str))
    state_digest = recovery_state_digest(state)
    selected_evidence = tuple(
        item for item in state.evidence if item.evidence_id in set(ordered_evidence)
    )
    evidence_digest = recovery_evidence_digest(selected_evidence)
    material = _plan_material(
        policy_version=RECOVERY_POLICY_VERSION,
        action=action,
        incident_id=incident_id,
        target=target,
        evidence_ids=ordered_evidence,
        evidence_digest=evidence_digest,
        expected_event_sha256=expected_event_sha256,
        expected_state_digest=state_digest,
    )
    plan_hash = _sha256(_canonical_json(material))
    return RecoveryPlan(
        plan_id=uuid5(_PLAN_NAMESPACE, plan_hash),
        plan_hash=plan_hash,
        policy_version=RECOVERY_POLICY_VERSION,
        action=action,
        incident_id=incident_id,
        target=target,
        evidence_ids=ordered_evidence,
        evidence_digest=evidence_digest,
        expected_event_sha256=expected_event_sha256,
        expected_state_digest=state_digest,
    )


def recovery_plan_identity_is_valid(plan: RecoveryPlan) -> bool:
    material = _plan_material(
        policy_version=plan.policy_version,
        action=plan.action,
        incident_id=plan.incident_id,
        target=plan.target,
        evidence_ids=plan.evidence_ids,
        evidence_digest=plan.evidence_digest,
        expected_event_sha256=plan.expected_event_sha256,
        expected_state_digest=plan.expected_state_digest,
    )
    expected_hash = _sha256(_canonical_json(material))
    return plan.plan_hash == expected_hash and plan.plan_id == uuid5(_PLAN_NAMESPACE, expected_hash)


def _decision(
    plan: RecoveryPlan,
    status: EligibilityStatus,
    *reasons: RecoveryRejectionReason,
    effect: ExpectedRecoveryEffect = ExpectedRecoveryEffect.NONE,
    event: OrderCreatedEvent | None = None,
    state_digest: str | None = None,
) -> EligibilityDecision:
    return EligibilityDecision(
        status=status,
        policy_version=RECOVERY_POLICY_VERSION,
        plan_id=plan.plan_id,
        action=plan.action,
        state_digest=state_digest or plan.expected_state_digest,
        expected_effect=effect,
        reasons=tuple(reasons),
        validated_event_id=event.event_id if event else None,
        validated_aggregate_id=event.aggregate_id if event else None,
    )


def _matching_evidence(
    plan: RecoveryPlan, state: RecoveryStateSnapshot
) -> tuple[list[RecoveryEvidence], EligibilityDecision | None]:
    evidence_by_id = {item.evidence_id: item for item in state.evidence}
    missing = [item for item in plan.evidence_ids if item not in evidence_by_id]
    if missing:
        return [], _decision(
            plan,
            EligibilityStatus.INSUFFICIENT_EVIDENCE,
            RecoveryRejectionReason.MISSING_EVIDENCE,
        )
    selected = [evidence_by_id[item] for item in plan.evidence_ids]
    if any(item.incident_id != plan.incident_id for item in selected):
        return [], _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.FOREIGN_EVIDENCE,
        )
    return selected, None


def _evidence_targets_plan(plan: RecoveryPlan, evidence: list[RecoveryEvidence]) -> bool:
    target = plan.target
    for item in evidence:
        event_matches = target.event_id is not None and item.event_id == target.event_id
        coordinate_matches = (
            target.kafka_coordinate is not None and item.kafka_coordinate == target.kafka_coordinate
        )
        if event_matches or coordinate_matches:
            return True
    return False


def _parse_authoritative_event(
    plan: RecoveryPlan, state: RecoveryStateSnapshot, evidence: list[RecoveryEvidence]
) -> tuple[OrderCreatedEvent | None, EligibilityDecision | None]:
    authoritative = state.authoritative_event
    if authoritative is None:
        return None, _decision(
            plan,
            EligibilityStatus.INSUFFICIENT_EVIDENCE,
            RecoveryRejectionReason.MISSING_AUTHORITATIVE_EVENT,
        )
    if plan.expected_event_sha256 is None:
        return None, _decision(
            plan,
            EligibilityStatus.INSUFFICIENT_EVIDENCE,
            RecoveryRejectionReason.MISSING_EVENT_HASH,
        )
    actual_hash = _sha256(authoritative.raw_event)
    if actual_hash != authoritative.sha256 or actual_hash != plan.expected_event_sha256:
        return None, _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.EVENT_HASH_MISMATCH,
        )
    if authoritative.evidence_id is None or authoritative.evidence_id not in plan.evidence_ids:
        return None, _decision(
            plan,
            EligibilityStatus.INSUFFICIENT_EVIDENCE,
            RecoveryRejectionReason.MISSING_EVIDENCE,
        )
    source_evidence = next(
        (item for item in evidence if item.evidence_id == authoritative.evidence_id), None
    )
    if source_evidence is None or source_evidence.content_sha256 is None:
        return None, _decision(
            plan,
            EligibilityStatus.INSUFFICIENT_EVIDENCE,
            RecoveryRejectionReason.MISSING_EVENT_HASH,
        )
    source_targets_event = source_evidence.event_id == plan.target.event_id
    source_targets_coordinate = (
        plan.target.kafka_coordinate is not None
        and source_evidence.kafka_coordinate == plan.target.kafka_coordinate
        and authoritative.kafka_coordinate == plan.target.kafka_coordinate
    )
    if not (source_targets_event or source_targets_coordinate):
        return None, _decision(
            plan,
            EligibilityStatus.INSUFFICIENT_EVIDENCE,
            RecoveryRejectionReason.EVIDENCE_TARGET_MISMATCH,
        )
    if source_evidence.content_sha256 != actual_hash:
        return None, _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.EVENT_HASH_MISMATCH,
        )
    try:
        decoded = json.loads(authoritative.raw_event)
    except (UnicodeDecodeError, json.JSONDecodeError):
        decoded = None
    if isinstance(decoded, dict) and "fault_injection" in decoded:
        return None, _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.POISON_EVENT,
        )
    try:
        event = parse_event(authoritative.raw_event, fault_enabled=True)
    except PermanentEventError as exc:
        if exc.incident_type == "UNSUPPORTED_SCHEMA_VERSION":
            reason = RecoveryRejectionReason.UNSUPPORTED_SCHEMA
        elif exc.incident_type == "BUSINESS_INVARIANT_VIOLATION":
            reason = RecoveryRejectionReason.INVALID_BUSINESS_INVARIANT
        elif exc.incident_type == "POISON_MESSAGE":
            reason = RecoveryRejectionReason.POISON_EVENT
        else:
            reason = RecoveryRejectionReason.MALFORMED_PAYLOAD
        return None, _decision(plan, EligibilityStatus.INELIGIBLE, reason)
    if event.event_id != plan.target.event_id or event.aggregate_id != plan.target.aggregate_id:
        return None, _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.EVENT_TARGET_MISMATCH,
        )
    return event, None


def _evaluate_replay(
    plan: RecoveryPlan, state: RecoveryStateSnapshot, event: OrderCreatedEvent
) -> EligibilityDecision:
    authoritative = state.authoritative_event
    if (
        authoritative.source
        in {
            AuthoritativeEventSource.BROKER,
            AuthoritativeEventSource.QUARANTINE,
        }
        and authoritative.kafka_coordinate != plan.target.kafka_coordinate
    ):
        return _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.EVENT_TARGET_MISMATCH,
            event=event,
        )
    if authoritative.source == AuthoritativeEventSource.QUARANTINE and not state.quarantined:
        return _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.NOT_QUARANTINED,
            event=event,
        )
    expected_payload = event.payload.model_dump(mode="json")
    processed = state.processed_event
    projection = state.projection
    if processed is not None:
        if (
            processed.event_id != event.event_id
            or processed.aggregate_id != event.aggregate_id
            or processed.event_type != event.event_type
        ):
            return _decision(
                plan,
                EligibilityStatus.INELIGIBLE,
                RecoveryRejectionReason.CONFLICTING_PROCESSED_EVENT,
                event=event,
            )
        if (
            projection is None
            or projection.order_id != event.aggregate_id
            or projection.source_event_id != event.event_id
            or _canonical_json(projection.payload) != _canonical_json(expected_payload)
        ):
            return _decision(
                plan,
                EligibilityStatus.INELIGIBLE,
                RecoveryRejectionReason.CONFLICTING_PROJECTION,
                event=event,
            )
        return _decision(
            plan,
            EligibilityStatus.ELIGIBLE,
            effect=ExpectedRecoveryEffect.NO_OP_ALREADY_APPLIED,
            event=event,
        )
    if projection is not None:
        return _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.CONFLICTING_PROJECTION,
            event=event,
        )
    return _decision(
        plan,
        EligibilityStatus.ELIGIBLE,
        effect=ExpectedRecoveryEffect.APPLY_EVENT,
        event=event,
    )


def _evaluate_outbox(
    plan: RecoveryPlan, state: RecoveryStateSnapshot, event: OrderCreatedEvent
) -> EligibilityDecision:
    authoritative = state.authoritative_event
    outbox = state.outbox_event
    if outbox is None:
        return _decision(
            plan,
            EligibilityStatus.INSUFFICIENT_EVIDENCE,
            RecoveryRejectionReason.MISSING_OUTBOX_EVENT,
            event=event,
        )
    expected_payload = event.payload.model_dump(mode="json")
    if (
        authoritative.source != AuthoritativeEventSource.OUTBOX
        or authoritative.outbox_event_id != plan.target.outbox_event_id
        or outbox.event_id != plan.target.outbox_event_id
        or outbox.event_id != event.event_id
        or outbox.aggregate_id != event.aggregate_id
        or outbox.event_type != ORDER_CREATED_EVENT_TYPE
        or outbox.schema_version != 1
        or _canonical_json(outbox.payload) != _canonical_json(expected_payload)
    ):
        return _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.OUTBOX_TARGET_MISMATCH,
            event=event,
        )
    effect = (
        ExpectedRecoveryEffect.NO_OP_ALREADY_PUBLISHED
        if outbox.published
        else ExpectedRecoveryEffect.RESCHEDULE_OUTBOX
    )
    return _decision(plan, EligibilityStatus.ELIGIBLE, effect=effect, event=event)


def evaluate_recovery_plan(plan: RecoveryPlan, state: RecoveryStateSnapshot) -> EligibilityDecision:
    actual_state_digest = recovery_state_digest(state)
    if plan.policy_version != RECOVERY_POLICY_VERSION:
        return _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.POLICY_VERSION_MISMATCH,
            state_digest=actual_state_digest,
        )
    if not recovery_plan_identity_is_valid(plan):
        return _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.INVALID_PLAN_IDENTIFIER,
            state_digest=actual_state_digest,
        )
    if plan.incident_id != state.incident_id:
        return _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.FOREIGN_EVIDENCE,
            state_digest=actual_state_digest,
        )
    if plan.expected_state_digest != actual_state_digest:
        return _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.STALE_STATE,
            state_digest=actual_state_digest,
        )
    evidence, evidence_decision = _matching_evidence(plan, state)
    if evidence_decision is not None:
        return evidence_decision
    if plan.action == RecoveryActionType.REQUEST_MANUAL_REVIEW:
        return _decision(
            plan,
            EligibilityStatus.ELIGIBLE,
            effect=ExpectedRecoveryEffect.REQUEST_MANUAL_REVIEW,
        )
    if not evidence:
        return _decision(
            plan,
            EligibilityStatus.INSUFFICIENT_EVIDENCE,
            RecoveryRejectionReason.MISSING_EVIDENCE,
        )
    if not _evidence_targets_plan(plan, evidence):
        return _decision(
            plan,
            EligibilityStatus.INSUFFICIENT_EVIDENCE,
            RecoveryRejectionReason.EVIDENCE_TARGET_MISMATCH,
        )
    if plan.action == RecoveryActionType.RETAIN_QUARANTINE:
        if not state.quarantined:
            return _decision(
                plan,
                EligibilityStatus.INELIGIBLE,
                RecoveryRejectionReason.NOT_QUARANTINED,
            )
        return _decision(
            plan,
            EligibilityStatus.ELIGIBLE,
            effect=ExpectedRecoveryEffect.RETAIN_QUARANTINE,
        )
    if plan.action not in _MUTATING_ACTIONS:
        return _decision(
            plan,
            EligibilityStatus.INELIGIBLE,
            RecoveryRejectionReason.UNSUPPORTED_ACTION,
        )
    event, event_decision = _parse_authoritative_event(plan, state, evidence)
    if event_decision is not None:
        return event_decision
    if plan.action == RecoveryActionType.REPLAY_VERIFIED_EVENT:
        return _evaluate_replay(plan, state, event)
    return _evaluate_outbox(plan, state, event)


def validate_recovery_plan(payload: dict[str, Any]) -> RecoveryPlan:
    """Validate untrusted plan input without interpreting unsupported actions."""
    try:
        return RecoveryPlan.model_validate(payload, strict=True)
    except ValidationError:
        raise
