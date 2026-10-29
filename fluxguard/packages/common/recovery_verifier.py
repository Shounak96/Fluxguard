import base64
import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from fluxguard.apps.worker.consumer import apply_event_in_transaction, parse_event
from fluxguard.apps.worker.outbox import event_bytes
from fluxguard.packages.common import recovery_policy
from fluxguard.packages.common.models import (
    ConsumerRetryState,
    Incident,
    IncidentEvidence,
    Order,
    OrderItem,
    OrderProjection,
    OutboxEvent,
    ProcessedEvent,
    QuarantinedEvent,
    RecoveryPlanEntity,
)
from fluxguard.packages.common.recoveries import (
    InvalidRecoveryTransitionError,
    get_recovery_plan_history,
    record_recovery_verification,
    recovery_evidence_from_row,
)
from fluxguard.packages.contracts.recovery import (
    AuthoritativeEventData,
    AuthoritativeEventSource,
    EligibilityDecision,
    EligibilityStatus,
    ExpectedRecoveryEffect,
    OutboxEventSnapshot,
    ProcessedEventSnapshot,
    ProjectionSnapshot,
    RecoveryActionType,
    RecoveryDryRunResult,
    RecoveryPlan,
    RecoveryStateSnapshot,
)

_BUSINESS_MODELS = (
    Order,
    OrderItem,
    OutboxEvent,
    ProcessedEvent,
    OrderProjection,
    QuarantinedEvent,
    ConsumerRetryState,
    Incident,
    IncidentEvidence,
)


class RecoveryVerificationError(RuntimeError):
    pass


class RecoverySnapshotBusyError(RecoveryVerificationError):
    pass


class RecoveryDryRunInvariantError(RecoveryVerificationError):
    pass


def _canonical_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return hashlib.sha256(value).hexdigest()
    raise TypeError(f"unsupported snapshot value: {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=_canonical_default,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def stable_advisory_lock_keys(plan: RecoveryPlan) -> tuple[int, ...]:
    identities = []
    if plan.target.event_id is not None:
        identities.append(f"event:{plan.target.event_id}")
    if plan.target.aggregate_id is not None:
        identities.append(f"aggregate:{plan.target.aggregate_id}")
    keys = set()
    for identity in identities:
        raw = hashlib.sha256(f"fluxguard-recovery:{identity}".encode()).digest()[:8]
        keys.add(int.from_bytes(raw, byteorder="big", signed=True))
    return tuple(sorted(keys))


def _acquire_target_locks(session: Session, plan: RecoveryPlan) -> None:
    for key in stable_advisory_lock_keys(plan):
        acquired = session.scalar(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": key})
        if not acquired:
            raise RecoverySnapshotBusyError("another verifier holds the target advisory lock")


def _decode_quarantine_payload(value: str | None) -> bytes | None:
    if value is None:
        return None
    if value.startswith("base64:"):
        try:
            return base64.b64decode(value.removeprefix("base64:"), validate=True)
        except ValueError:
            return None
    return value.encode()


def _target_evidence_id(plan: RecoveryPlan, state_evidence, content_hash: str) -> UUID | None:
    for item in state_evidence:
        event_matches = plan.target.event_id is not None and item.event_id == plan.target.event_id
        coordinate_matches = (
            plan.target.kafka_coordinate is not None
            and item.kafka_coordinate == plan.target.kafka_coordinate
        )
        if (event_matches or coordinate_matches) and item.content_sha256 == content_hash:
            return item.evidence_id
    for item in state_evidence:
        if item.evidence_id in plan.evidence_ids:
            return item.evidence_id
    return None


def load_authoritative_recovery_snapshot(
    session: Session, plan: RecoveryPlan
) -> RecoveryStateSnapshot:
    if session.get(Incident, plan.incident_id) is None:
        raise LookupError("incident not found")
    evidence_rows = []
    if plan.evidence_ids:
        evidence_rows = session.scalars(
            select(IncidentEvidence).where(IncidentEvidence.evidence_id.in_(plan.evidence_ids))
        ).all()
    state_evidence = tuple(
        recovery_evidence_from_row(item)
        for item in sorted(evidence_rows, key=lambda item: str(item.evidence_id))
    )
    processed = (
        session.get(ProcessedEvent, plan.target.event_id)
        if plan.target.event_id is not None
        else None
    )
    projection = (
        session.get(OrderProjection, plan.target.aggregate_id)
        if plan.target.aggregate_id is not None
        else None
    )
    outbox_id = plan.target.outbox_event_id or plan.target.event_id
    outbox = None
    if outbox_id is not None:
        statement = select(OutboxEvent).where(OutboxEvent.event_id == outbox_id)
        if plan.action in {
            RecoveryActionType.REPLAY_VERIFIED_EVENT,
            RecoveryActionType.RESCHEDULE_OUTBOX_EVENT,
        }:
            statement = statement.with_for_update(nowait=True)
        try:
            outbox = session.scalar(statement)
        except DBAPIError as exc:
            raise RecoverySnapshotBusyError("target outbox row is locked by the publisher") from exc
    quarantine = None
    coordinate = plan.target.kafka_coordinate
    if coordinate is not None:
        quarantine = session.scalar(
            select(QuarantinedEvent).where(
                QuarantinedEvent.topic == coordinate.topic,
                QuarantinedEvent.partition == coordinate.partition,
                QuarantinedEvent.offset == coordinate.offset,
            )
        )

    candidates: list[AuthoritativeEventData] = []
    if quarantine is not None:
        raw = _decode_quarantine_payload(quarantine.raw_payload)
        if raw is not None:
            digest = _sha256_bytes(raw)
            candidates.append(
                AuthoritativeEventData(
                    source=AuthoritativeEventSource.QUARANTINE,
                    raw_event=raw,
                    sha256=digest,
                    evidence_id=_target_evidence_id(plan, state_evidence, digest),
                    kafka_coordinate=coordinate,
                )
            )
    if outbox is not None:
        raw = event_bytes(outbox)
        digest = _sha256_bytes(raw)
        candidates.append(
            AuthoritativeEventData(
                source=AuthoritativeEventSource.OUTBOX,
                raw_event=raw,
                sha256=digest,
                evidence_id=_target_evidence_id(plan, state_evidence, digest),
                outbox_event_id=outbox.event_id,
            )
        )
    if plan.action == RecoveryActionType.RESCHEDULE_OUTBOX_EVENT:
        candidates = [item for item in candidates if item.source == AuthoritativeEventSource.OUTBOX]
    authoritative = next(
        (item for item in candidates if item.sha256 == plan.expected_event_sha256),
        candidates[0] if candidates else None,
    )
    last_error_hash = (
        hashlib.sha256(outbox.last_error.encode()).hexdigest()
        if outbox is not None and outbox.last_error
        else None
    )
    return RecoveryStateSnapshot(
        incident_id=plan.incident_id,
        evidence=state_evidence,
        authoritative_event=authoritative,
        processed_event=(
            ProcessedEventSnapshot(
                event_id=processed.event_id,
                event_type=processed.event_type,
                aggregate_id=processed.aggregate_id,
            )
            if processed is not None
            else None
        ),
        projection=(
            ProjectionSnapshot(
                order_id=projection.order_id,
                source_event_id=projection.source_event_id,
                payload=projection.payload,
            )
            if projection is not None
            else None
        ),
        outbox_event=(
            OutboxEventSnapshot(
                event_id=outbox.event_id,
                event_type=outbox.event_type,
                schema_version=outbox.schema_version,
                aggregate_id=outbox.aggregate_id,
                payload=outbox.payload,
                published=outbox.published_at is not None,
                attempts=outbox.attempts,
                next_attempt_at=outbox.next_attempt_at,
                last_error_sha256=last_error_hash,
            )
            if outbox is not None
            else None
        ),
        quarantined=quarantine is not None,
    )


def _table_snapshot(session: Session, model) -> dict[str, str]:
    mapper = sqlalchemy_inspect(model)
    primary_keys = [mapper.get_property_by_column(column).key for column in mapper.primary_key]
    attributes = [item.key for item in mapper.column_attrs]
    rows = session.scalars(select(model)).all()
    snapshot = {}
    for row in rows:
        key = "|".join(str(getattr(row, name)) for name in primary_keys)
        values = {name: getattr(row, name) for name in attributes}
        snapshot[key] = hashlib.sha256(_canonical_json(values).encode()).hexdigest()
    return snapshot


def _business_snapshot(session: Session) -> dict[str, dict[str, str]]:
    return {model.__tablename__: _table_snapshot(session, model) for model in _BUSINESS_MODELS}


def _changed_rows(before: dict[str, dict[str, str]], after: dict[str, dict[str, str]]) -> set[str]:
    changed = set()
    for table in before.keys() | after.keys():
        before_rows = before.get(table, {})
        after_rows = after.get(table, {})
        for key in before_rows.keys() | after_rows.keys():
            if before_rows.get(key) != after_rows.get(key):
                changed.add(f"{table}:{key}")
    return changed


def _assert_write_set(changed: set[str], allowed: set[str]) -> None:
    unexpected = changed - allowed
    if unexpected:
        raise RecoveryDryRunInvariantError(
            f"simulation changed rows outside its declared write set: {sorted(unexpected)}"
        )


def _simulate_eligible_action(
    session: Session,
    plan: RecoveryPlan,
    state: RecoveryStateSnapshot,
    decision: EligibilityDecision,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    before = _business_snapshot(session)
    expected_changes: set[str] = set()
    postconditions = []
    if decision.expected_effect == ExpectedRecoveryEffect.APPLY_EVENT:
        if state.authoritative_event is None:
            raise RecoveryDryRunInvariantError("eligible replay has no authoritative event")
        event = parse_event(state.authoritative_event.raw_event, fault_enabled=True)
        if not apply_event_in_transaction(session, event):
            raise RecoveryDryRunInvariantError("fresh replay unexpectedly became a duplicate")
        session.flush()
        marker = session.get(ProcessedEvent, event.event_id)
        projection = session.get(OrderProjection, event.aggregate_id)
        if marker is None or projection is None or projection.source_event_id != event.event_id:
            raise RecoveryDryRunInvariantError("replay postconditions were not satisfied")
        expected_changes = {
            f"processed_events:{event.event_id}",
            f"order_projections:{event.aggregate_id}",
        }
        postconditions.extend(("processed marker would exist", "exact projection would exist"))
    elif decision.expected_effect == ExpectedRecoveryEffect.RESCHEDULE_OUTBOX:
        outbox = session.get(OutboxEvent, plan.target.outbox_event_id)
        if outbox is None or outbox.published_at is not None:
            raise RecoveryDryRunInvariantError("outbox row is no longer reschedulable")
        outbox.next_attempt_at = outbox.occurred_at
        session.flush()
        expected_changes = {f"outbox_events:{outbox.event_id}"}
        postconditions.append("outbox row would become immediately retryable")
    elif decision.expected_effect == ExpectedRecoveryEffect.NO_OP_ALREADY_APPLIED:
        postconditions.append("existing processed marker and projection remain unchanged")
    elif decision.expected_effect == ExpectedRecoveryEffect.NO_OP_ALREADY_PUBLISHED:
        postconditions.append("published outbox row remains unchanged")
    elif decision.expected_effect == ExpectedRecoveryEffect.RETAIN_QUARANTINE:
        postconditions.append("quarantine record remains unchanged")
    elif decision.expected_effect == ExpectedRecoveryEffect.REQUEST_MANUAL_REVIEW:
        postconditions.append("business state remains unchanged")
    session.flush()
    after = _business_snapshot(session)
    changed = _changed_rows(before, after)
    _assert_write_set(changed, expected_changes)
    if (
        decision.expected_effect
        in {
            ExpectedRecoveryEffect.APPLY_EVENT,
            ExpectedRecoveryEffect.RESCHEDULE_OUTBOX,
        }
        and changed != expected_changes
    ):
        raise RecoveryDryRunInvariantError(
            "simulation did not produce its complete declared write set"
        )
    if (
        decision.expected_effect
        not in {
            ExpectedRecoveryEffect.APPLY_EVENT,
            ExpectedRecoveryEffect.RESCHEDULE_OUTBOX,
        }
        and changed
    ):
        raise RecoveryDryRunInvariantError("non-mutating action changed business state")
    return tuple(sorted(changed)), tuple(postconditions)


def _run_rollback_simulation(
    session_factory: sessionmaker[Session], plan_id: UUID
) -> tuple[RecoveryPlan, EligibilityDecision, tuple[str, ...], tuple[str, ...]]:
    session = session_factory()
    try:
        session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
        isolation = session.scalar(text("SHOW transaction_isolation"))
        if isolation != "repeatable read":
            raise RecoveryVerificationError("dry run requires PostgreSQL REPEATABLE READ")
        session.execute(text("SET LOCAL lock_timeout = '500ms'"))
        session.execute(text("SET LOCAL statement_timeout = '5s'"))
        entity = session.get(RecoveryPlanEntity, plan_id)
        if entity is None:
            raise LookupError("recovery plan not found")
        plan = RecoveryPlan.model_validate(entity.canonical_inputs)
        _acquire_target_locks(session, plan)
        state = load_authoritative_recovery_snapshot(session, plan)
        decision = recovery_policy.evaluate_recovery_plan(plan, state)
        if decision.status == EligibilityStatus.ELIGIBLE:
            write_set, postconditions = _simulate_eligible_action(session, plan, state, decision)
        else:
            before = _business_snapshot(session)
            after = _business_snapshot(session)
            if before != after:
                raise RecoveryDryRunInvariantError("rejected plan changed business state")
            write_set = ()
            postconditions = ("unsafe action rejected without business mutation",)
        return plan, decision, write_set, postconditions
    finally:
        session.rollback()
        session.close()


def verify_recovery_plan_dry_run(
    session_factory: sessionmaker[Session], plan_id: UUID
) -> RecoveryDryRunResult:
    plan, decision, write_set, postconditions = _run_rollback_simulation(session_factory, plan_id)
    try:
        with session_factory() as session, session.begin():
            persisted = record_recovery_verification(
                session,
                plan_id,
                decision,
                verifier_version=recovery_policy.RECOVERY_POLICY_VERSION,
                simulated_write_set=write_set,
                postconditions=postconditions,
            )
    except InvalidRecoveryTransitionError:
        with session_factory() as session:
            history = get_recovery_plan_history(session, plan_id)
            if not history.verifications:
                raise
            persisted = history.verifications[0]
    return RecoveryDryRunResult(
        plan_id=plan_id,
        decision=decision,
        simulated_write_set=write_set,
        postconditions=postconditions,
        persisted_verification=persisted,
    )
