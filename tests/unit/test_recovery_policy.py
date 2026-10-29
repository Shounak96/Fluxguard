import hashlib
import json
from copy import deepcopy
from uuid import UUID

import pytest
from pydantic import ValidationError

from fluxguard.packages.common.recovery_policy import (
    build_recovery_plan,
    evaluate_recovery_plan,
)
from fluxguard.packages.contracts.recovery import (
    AuthoritativeEventData,
    AuthoritativeEventSource,
    EligibilityStatus,
    ExpectedRecoveryEffect,
    KafkaCoordinate,
    OutboxEventSnapshot,
    ProcessedEventSnapshot,
    ProjectionSnapshot,
    RecoveryActionType,
    RecoveryEvidence,
    RecoveryPlan,
    RecoveryRejectionReason,
    RecoveryStateSnapshot,
    RecoveryTarget,
)

INCIDENT_ID = UUID("00000000-0000-4000-8000-000000000001")
FOREIGN_INCIDENT_ID = UUID("00000000-0000-4000-8000-000000000002")
EVIDENCE_ID = UUID("00000000-0000-4000-8000-000000000003")
EVENT_ID = UUID("00000000-0000-4000-8000-000000000004")
ORDER_ID = UUID("00000000-0000-4000-8000-000000000005")
COORDINATE = KafkaCoordinate(topic="orders.created.v1", partition=0, offset=42)


def event_payload(*, schema_version: int = 1, total: str = "49.98") -> dict:
    return {
        "event_id": str(EVENT_ID),
        "event_type": "orders.created.v1",
        "schema_version": schema_version,
        "aggregate_id": str(ORDER_ID),
        "occurred_at": "2026-01-01T00:00:00Z",
        "payload": {
            "order_id": str(ORDER_ID),
            "customer_id": "cust-recovery",
            "items": [
                {
                    "product_id": "product-1",
                    "quantity": 2,
                    "unit_price": "24.99",
                    "line_total": "49.98",
                }
            ],
            "currency": "USD",
            "total": total,
            "created_at": "2026-01-01T00:00:00Z",
        },
    }


def raw_event(payload: dict | None = None) -> bytes:
    return json.dumps(payload or event_payload(), sort_keys=True, separators=(",", ":")).encode()


def state_for(
    *,
    raw: bytes | None = None,
    incident_id: UUID = INCIDENT_ID,
    evidence_incident_id: UUID = INCIDENT_ID,
    processed: ProcessedEventSnapshot | None = None,
    projection: ProjectionSnapshot | None = None,
    quarantined: bool = True,
    source: AuthoritativeEventSource = AuthoritativeEventSource.QUARANTINE,
    outbox: OutboxEventSnapshot | None = None,
) -> RecoveryStateSnapshot:
    raw = raw if raw is not None else raw_event()
    digest = hashlib.sha256(raw).hexdigest()
    return RecoveryStateSnapshot(
        incident_id=incident_id,
        evidence=(
            RecoveryEvidence(
                evidence_id=EVIDENCE_ID,
                incident_id=evidence_incident_id,
                event_id=EVENT_ID,
                kafka_coordinate=COORDINATE,
                content_sha256=digest,
            ),
        ),
        authoritative_event=AuthoritativeEventData(
            source=source,
            raw_event=raw,
            sha256=digest,
            evidence_id=EVIDENCE_ID,
            kafka_coordinate=(COORDINATE if source != AuthoritativeEventSource.OUTBOX else None),
            outbox_event_id=(EVENT_ID if source == AuthoritativeEventSource.OUTBOX else None),
        ),
        processed_event=processed,
        projection=projection,
        outbox_event=outbox,
        quarantined=quarantined,
    )


def plan_for(
    state: RecoveryStateSnapshot,
    *,
    action: RecoveryActionType = RecoveryActionType.REPLAY_VERIFIED_EVENT,
    evidence_ids: list[UUID] | None = None,
):
    authoritative = state.authoritative_event
    target = RecoveryTarget(
        event_id=EVENT_ID,
        aggregate_id=ORDER_ID,
        kafka_coordinate=(
            COORDINATE if action != RecoveryActionType.RESCHEDULE_OUTBOX_EVENT else None
        ),
        outbox_event_id=(
            EVENT_ID if action == RecoveryActionType.RESCHEDULE_OUTBOX_EVENT else None
        ),
    )
    return build_recovery_plan(
        action=action,
        incident_id=INCIDENT_ID,
        target=target,
        evidence_ids=[EVIDENCE_ID] if evidence_ids is None else evidence_ids,
        state=state,
        expected_event_sha256=(authoritative.sha256 if authoritative is not None else None),
    )


def projection_for(payload: dict | None = None) -> ProjectionSnapshot:
    return ProjectionSnapshot(
        order_id=ORDER_ID,
        source_event_id=EVENT_ID,
        payload=(payload or event_payload()["payload"]),
    )


def test_valid_replay_candidate_is_deterministic_and_eligible() -> None:
    state = state_for()
    first = plan_for(state)
    second = plan_for(state)
    assert first == second
    decision = evaluate_recovery_plan(first, state)
    assert decision.status == EligibilityStatus.ELIGIBLE
    assert decision.expected_effect == ExpectedRecoveryEffect.APPLY_EVENT
    assert decision.reasons == ()


def test_already_processed_identical_event_is_an_eligible_noop() -> None:
    processed = ProcessedEventSnapshot(
        event_id=EVENT_ID,
        event_type="orders.created.v1",
        aggregate_id=ORDER_ID,
    )
    state = state_for(processed=processed, projection=projection_for())
    decision = evaluate_recovery_plan(plan_for(state), state)
    assert decision.status == EligibilityStatus.ELIGIBLE
    assert decision.expected_effect == ExpectedRecoveryEffect.NO_OP_ALREADY_APPLIED


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (raw_event(event_payload(schema_version=99)), RecoveryRejectionReason.UNSUPPORTED_SCHEMA),
        (b"not-json", RecoveryRejectionReason.MALFORMED_PAYLOAD),
        (
            raw_event(event_payload(total="50.00")),
            RecoveryRejectionReason.INVALID_BUSINESS_INVARIANT,
        ),
    ],
)
def test_unsafe_event_payloads_are_ineligible(raw: bytes, reason: RecoveryRejectionReason) -> None:
    state = state_for(raw=raw)
    decision = evaluate_recovery_plan(plan_for(state), state)
    assert decision.status == EligibilityStatus.INELIGIBLE
    assert decision.reasons == (reason,)


def test_fault_injection_marker_and_hash_mismatch_are_ineligible() -> None:
    poison = event_payload()
    poison["fault_injection"] = {"scenario": "poison_message"}
    poison_state = state_for(raw=raw_event(poison))
    poison_decision = evaluate_recovery_plan(plan_for(poison_state), poison_state)
    assert poison_decision.reasons == (RecoveryRejectionReason.POISON_EVENT,)

    valid_state = state_for()
    bad_authoritative = valid_state.authoritative_event.model_copy(update={"sha256": "0" * 64})
    hash_state = valid_state.model_copy(update={"authoritative_event": bad_authoritative})
    hash_decision = evaluate_recovery_plan(plan_for(hash_state), hash_state)
    assert hash_decision.reasons == (RecoveryRejectionReason.EVENT_HASH_MISMATCH,)


def test_missing_and_foreign_evidence_fail_closed() -> None:
    state = state_for()
    missing_plan = plan_for(state, evidence_ids=[UUID("00000000-0000-4000-8000-000000000099")])
    missing = evaluate_recovery_plan(missing_plan, state)
    assert missing.status == EligibilityStatus.INSUFFICIENT_EVIDENCE
    assert missing.reasons == (RecoveryRejectionReason.MISSING_EVIDENCE,)

    foreign_state = state_for(evidence_incident_id=FOREIGN_INCIDENT_ID)
    foreign = evaluate_recovery_plan(plan_for(foreign_state), foreign_state)
    assert foreign.status == EligibilityStatus.INELIGIBLE
    assert foreign.reasons == (RecoveryRejectionReason.FOREIGN_EVIDENCE,)


def test_stale_and_conflicting_state_are_ineligible() -> None:
    original = state_for()
    plan = plan_for(original)
    changed = state_for(quarantined=False)
    stale = evaluate_recovery_plan(plan, changed)
    assert stale.reasons == (RecoveryRejectionReason.STALE_STATE,)

    conflict_payload = deepcopy(event_payload()["payload"])
    conflict_payload["customer_id"] = "different"
    conflicting = state_for(projection=projection_for(conflict_payload))
    conflict = evaluate_recovery_plan(plan_for(conflicting), conflicting)
    assert conflict.status == EligibilityStatus.INELIGIBLE
    assert conflict.reasons == (RecoveryRejectionReason.CONFLICTING_PROJECTION,)


def test_unsupported_action_is_rejected_by_closed_contract() -> None:
    state = state_for()
    valid = plan_for(state).model_dump(mode="json")
    valid["action"] = "DELETE_PROJECTION"
    with pytest.raises(ValidationError):
        RecoveryPlan.model_validate(valid)


def test_non_mutating_decisions_are_typed_and_do_not_require_event_parsing() -> None:
    state = state_for(raw=b"not-json")
    retained = evaluate_recovery_plan(
        plan_for(state, action=RecoveryActionType.RETAIN_QUARANTINE), state
    )
    assert retained.status == EligibilityStatus.ELIGIBLE
    assert retained.expected_effect == ExpectedRecoveryEffect.RETAIN_QUARANTINE

    no_evidence = RecoveryStateSnapshot(incident_id=INCIDENT_ID)
    manual = evaluate_recovery_plan(
        plan_for(
            no_evidence,
            action=RecoveryActionType.REQUEST_MANUAL_REVIEW,
            evidence_ids=[],
        ),
        no_evidence,
    )
    assert manual.status == EligibilityStatus.ELIGIBLE
    assert manual.expected_effect == ExpectedRecoveryEffect.REQUEST_MANUAL_REVIEW


def test_outbox_reschedule_requires_matching_unpublished_authoritative_event() -> None:
    payload = event_payload()
    outbox = OutboxEventSnapshot(
        event_id=EVENT_ID,
        event_type="orders.created.v1",
        schema_version=1,
        aggregate_id=ORDER_ID,
        payload=payload["payload"],
        published=False,
    )
    state = state_for(
        source=AuthoritativeEventSource.OUTBOX,
        quarantined=False,
        outbox=outbox,
    )
    decision = evaluate_recovery_plan(
        plan_for(state, action=RecoveryActionType.RESCHEDULE_OUTBOX_EVENT), state
    )
    assert decision.status == EligibilityStatus.ELIGIBLE
    assert decision.expected_effect == ExpectedRecoveryEffect.RESCHEDULE_OUTBOX
