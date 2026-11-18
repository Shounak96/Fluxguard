import hashlib
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from threading import Event
from typing import Any
from uuid import UUID

from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from fluxguard.apps.worker.consumer import apply_event, parse_event, safe_text
from fluxguard.apps.worker.outbox import KafkaEventPublisher, event_bytes, publish_pending
from fluxguard.packages.common import recovery_verifier
from fluxguard.packages.common.benchmark_observability import load_recovery_audit_trace
from fluxguard.packages.common.benchmark_runner import BenchmarkScenarioContext
from fluxguard.packages.common.incidents import Detection, record_incident
from fluxguard.packages.common.models import (
    IncidentEvidence,
    IncidentSeverity,
    OrderProjection,
    OutboxEvent,
    ProcessedEvent,
    QuarantinedEvent,
    RecoveryExecutionEvent,
    RecoveryLifecycleEvent,
    RecoveryPlanEntity,
)
from fluxguard.packages.common.recoveries import (
    RecoveryEvidenceOwnershipError,
    create_recovery_plan,
    recovery_evidence_from_row,
)
from fluxguard.packages.common.recovery_approvals import (
    AuthenticatedRecoveryOperator,
    RecoveryApprovalStaleError,
    approve_recovery_plan,
)
from fluxguard.packages.common.recovery_executor import (
    RecoveryExecutionAuthorizer,
    RecoveryExecutionConflictError,
    RecoveryExecutionError,
    RecoveryExecutionRejectedError,
    execute_recovery_plan,
)
from fluxguard.packages.common.recovery_policy import build_recovery_plan
from fluxguard.packages.contracts.benchmarks import (
    AssertionResults,
    BenchmarkOutcome,
    BenchmarkStage,
    EvidenceClass,
    SanitizedError,
    ScenarioFamily,
    ScenarioIdentity,
    ScenarioResult,
    StageTiming,
)
from fluxguard.packages.contracts.recovery import (
    EligibilityStatus,
    KafkaCoordinate,
    RecoveryActionType,
    RecoveryExecutionStatus,
    RecoveryLifecycleState,
    RecoveryPlanSource,
    RecoveryStateSnapshot,
    RecoveryTarget,
)


@dataclass(frozen=True)
class RecoveryBenchmarkFixture:
    incident_id: UUID
    evidence_id: UUID
    event_id: UUID
    order_id: UUID
    coordinate: KafkaCoordinate
    raw: bytes
    raw_hash: str


@dataclass
class ScenarioObservation:
    actual_outcome: BenchmarkOutcome
    assertions: AssertionResults
    timings: list[StageTiming] = field(default_factory=list)
    error: SanitizedError | None = None
    limitations: list[str] = field(default_factory=list)


@dataclass
class ScenarioTraceState:
    incident_id: UUID | None = None
    plan_id: UUID | None = None
    execution_id: UUID | None = None
    additional_reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExpandedScenarioSpec:
    scenario_id: str
    scenario_family: ScenarioFamily
    variation: str
    trial: int
    trial_seed: int
    expected_outcome: BenchmarkOutcome
    concurrency: bool = False


def _expanded_trial_seed(
    base_seed: int,
    family: ScenarioFamily,
    variation: str,
    trial: int,
) -> int:
    material = f"{base_seed}:{family.value}:{variation}:{trial}".encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "big") & (2**63 - 1)


def expanded_recovery_scenario_schedule(
    *,
    seed: int,
    family_trials: int,
    concurrency_trials: int,
) -> tuple[ExpandedScenarioSpec, ...]:
    if seed < 0:
        raise ValueError("benchmark seed must be non-negative")
    if not 1 <= family_trials <= 100 or not 1 <= concurrency_trials <= 100:
        raise ValueError("expanded benchmark trial counts must be between 1 and 100")

    eligible = (
        ("approved-eligible-event-replay", BenchmarkOutcome.RECOVERED),
        ("approved-eligible-outbox-reschedule", BenchmarkOutcome.RECOVERED),
    )
    unsafe = (
        ("unsupported-schema-rejection", BenchmarkOutcome.SAFELY_REJECTED),
        ("malformed-event-rejection", BenchmarkOutcome.SAFELY_REJECTED),
        ("invalid-financial-total-rejection", BenchmarkOutcome.SAFELY_REJECTED),
        ("missing-evidence-rejection", BenchmarkOutcome.SAFELY_REJECTED),
        ("foreign-evidence-rejection", BenchmarkOutcome.SAFELY_REJECTED),
        ("stale-approval-rejection", BenchmarkOutcome.SAFELY_REJECTED),
        ("expired-approval-rejection", BenchmarkOutcome.SAFELY_REJECTED),
    )
    no_op = (("verified-duplicate-event-no-op", BenchmarkOutcome.VERIFIED_NO_OP),)
    concurrency = (
        ("two-executors", BenchmarkOutcome.RECOVERED),
        ("consumer-versus-executor", BenchmarkOutcome.OPERATIONAL_FAILURE),
        ("publisher-versus-executor", BenchmarkOutcome.RECOVERED),
    )

    specs: list[ExpandedScenarioSpec] = []

    def append_family(
        family: ScenarioFamily,
        variations: tuple[tuple[str, BenchmarkOutcome], ...],
    ) -> None:
        offset = _expanded_trial_seed(seed, family, "schedule", 1) % len(variations)
        for trial in range(1, family_trials + 1):
            variation, expected = variations[(offset + trial - 1) % len(variations)]
            specs.append(
                ExpandedScenarioSpec(
                    scenario_id=variation,
                    scenario_family=family,
                    variation=variation,
                    trial=trial,
                    trial_seed=_expanded_trial_seed(seed, family, variation, trial),
                    expected_outcome=expected,
                )
            )

    append_family(ScenarioFamily.ELIGIBLE_RECOVERY, eligible)
    append_family(ScenarioFamily.UNSAFE_REJECTION, unsafe)
    append_family(ScenarioFamily.DUPLICATE_NO_OP, no_op)
    for variation, expected in concurrency:
        for trial in range(1, concurrency_trials + 1):
            specs.append(
                ExpandedScenarioSpec(
                    scenario_id=variation,
                    scenario_family=ScenarioFamily.CONCURRENCY_SAFETY,
                    variation=variation,
                    trial=trial,
                    trial_seed=_expanded_trial_seed(
                        seed,
                        ScenarioFamily.CONCURRENCY_SAFETY,
                        variation,
                        trial,
                    ),
                    expected_outcome=expected,
                    concurrency=True,
                )
            )
    return tuple(specs)


class IsolatedBenchmarkAuthorizer(RecoveryExecutionAuthorizer):
    def __init__(self, context: BenchmarkScenarioContext) -> None:
        context.ownership.validate()
        if context.ownership.names.database_name not in context.database_url:
            raise ValueError("benchmark authorizer requires the owned disposable database")
        self._run_id = context.ownership.run_id

    def authorize(self, _plan_id: UUID, _execution_id: UUID) -> AuthenticatedRecoveryOperator:
        return AuthenticatedRecoveryOperator(
            subject=f"benchmark-test:{self._run_id}",
            identity_provider="isolated-benchmark-test-only",
        )


class RecoverySafetyScenarioSuite:
    def __init__(self, context: BenchmarkScenarioContext) -> None:
        context.ownership.validate()
        if context.ownership.names.database_name not in context.database_url:
            raise ValueError("scenario database is outside the owned benchmark namespace")
        self.context = context
        self.engine = create_engine(context.database_url, pool_pre_ping=True)
        self.factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.authorizer = IsolatedBenchmarkAuthorizer(context)
        self.random = random.Random(context.seed)
        self.offset = 1_000_000
        self.trace = ScenarioTraceState()

    def close(self) -> None:
        self.engine.dispose()

    def _uuid(self) -> UUID:
        return UUID(int=self.random.getrandbits(128), version=4)

    def _coordinate(self) -> KafkaCoordinate:
        self.offset += 1
        return KafkaCoordinate(
            topic=self.context.ownership.names.kafka_topic,
            partition=0,
            offset=self.offset,
        )

    def _event_payload(
        self,
        event_id: UUID,
        order_id: UUID,
        *,
        schema_version: int = 1,
        total: str = "49.98",
    ) -> dict[str, Any]:
        return {
            "event_id": str(event_id),
            "event_type": "orders.created.v1",
            "schema_version": schema_version,
            "aggregate_id": str(order_id),
            "occurred_at": "2026-01-01T00:00:00Z",
            "payload": {
                "order_id": str(order_id),
                "customer_id": "benchmark-customer",
                "items": [
                    {
                        "product_id": "benchmark-product",
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

    @staticmethod
    def _raw(payload: dict[str, Any]) -> bytes:
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()

    def _create_source(
        self,
        *,
        source: str,
        scenario: str,
        raw: bytes | None = None,
        event_id: UUID | None = None,
        order_id: UUID | None = None,
    ) -> RecoveryBenchmarkFixture:
        event_id = event_id or self._uuid()
        order_id = order_id or self._uuid()
        coordinate = self._coordinate()
        outbox = None
        if source == "outbox":
            payload = self._event_payload(event_id, order_id)
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
            raw = self._raw(self._event_payload(event_id, order_id))
        raw_hash = hashlib.sha256(raw).hexdigest()
        with self.factory() as session, session.begin():
            if outbox is not None:
                session.add(outbox)
            else:
                session.add(
                    QuarantinedEvent(
                        topic=coordinate.topic,
                        partition=coordinate.partition,
                        offset=coordinate.offset,
                        message_key=str(order_id),
                        raw_payload=safe_text(raw),
                        error_type="BENCHMARK_RECOVERY_SOURCE",
                        error_detail="isolated benchmark fixture",
                    )
                )
            incident, _ = record_incident(
                session,
                Detection(
                    incident_type="BENCHMARK_RECOVERY_SCENARIO",
                    severity=IncidentSeverity.MEDIUM,
                    error_code="BENCHMARK_RECOVERY_SCENARIO",
                    summary="isolated recovery safety benchmark",
                    source_component="benchmark_runner",
                    fingerprint_scope={"run": str(self.context.ownership.run_id), "case": scenario},
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
            incident_id = incident.incident_id
        self.trace.incident_id = incident_id
        return RecoveryBenchmarkFixture(
            incident_id=incident_id,
            evidence_id=evidence_id,
            event_id=event_id,
            order_id=order_id,
            coordinate=coordinate,
            raw=raw,
            raw_hash=raw_hash,
        )

    @staticmethod
    def _target(
        fixture: RecoveryBenchmarkFixture,
        action: RecoveryActionType,
    ) -> RecoveryTarget:
        return RecoveryTarget(
            event_id=fixture.event_id,
            aggregate_id=fixture.order_id,
            kafka_coordinate=fixture.coordinate,
            outbox_event_id=(
                fixture.event_id if action == RecoveryActionType.RESCHEDULE_OUTBOX_EVENT else None
            ),
        )

    def _persist_plan(
        self,
        fixture: RecoveryBenchmarkFixture,
        action: RecoveryActionType,
    ):
        with self.factory() as session:
            evidence = recovery_evidence_from_row(
                session.get(IncidentEvidence, fixture.evidence_id)
            )
        provisional = build_recovery_plan(
            action=action,
            incident_id=fixture.incident_id,
            target=self._target(fixture, action),
            evidence_ids=[fixture.evidence_id],
            state=RecoveryStateSnapshot(
                incident_id=fixture.incident_id,
                evidence=(evidence,),
            ),
            expected_event_sha256=fixture.raw_hash,
        )
        with self.factory() as session, session.begin():
            snapshot = recovery_verifier.load_authoritative_recovery_snapshot(session, provisional)
        plan = build_recovery_plan(
            action=action,
            incident_id=fixture.incident_id,
            target=self._target(fixture, action),
            evidence_ids=[fixture.evidence_id],
            state=snapshot,
            expected_event_sha256=fixture.raw_hash,
        )
        with self.factory() as session, session.begin():
            create_recovery_plan(
                session,
                plan,
                source=RecoveryPlanSource.SYSTEM,
                created_by="isolated-benchmark",
            )
        self.trace.plan_id = plan.plan_id
        return plan

    @staticmethod
    def _timed(stage: BenchmarkStage, operation, timings: list[StageTiming]):
        started = time.monotonic()
        try:
            return operation()
        finally:
            timings.append(
                StageTiming(
                    stage=stage,
                    duration_ms=(time.monotonic() - started) * 1000,
                )
            )

    def _approve(self, plan, timings: list[StageTiming], *, now=None):
        def operation():
            with self.factory() as session:
                return approve_recovery_plan(
                    session,
                    plan.plan_id,
                    operator=self.authorizer.authorize(plan.plan_id, self._uuid()),
                    idempotency_key=f"benchmark-approval-{plan.plan_id}",
                    now=now,
                )

        return self._timed(BenchmarkStage.APPROVAL, operation, timings)

    def _execute(self, plan, timings: list[StageTiming], *, execution_id=None, now=None):
        execution_id = execution_id or self._uuid()
        self.trace.execution_id = execution_id
        result = self._timed(
            BenchmarkStage.EXECUTION,
            lambda: execute_recovery_plan(
                self.factory,
                plan.plan_id,
                execution_id,
                expected_plan_hash=plan.plan_hash,
                authorizer=self.authorizer,
                now=now,
            ),
            timings,
        )
        return result, execution_id

    def _verify(self, plan, timings: list[StageTiming]):
        return self._timed(
            BenchmarkStage.VERIFICATION,
            lambda: recovery_verifier.verify_recovery_plan_dry_run(self.factory, plan.plan_id),
            timings,
        )

    def _snapshot(self):
        with self.factory() as session:
            return recovery_verifier._business_snapshot(session)

    def _watermark(self) -> int:
        consumer = Consumer(
            {
                "bootstrap.servers": self.context.kafka_bootstrap_servers,
                "group.id": f"{self.context.ownership.names.kafka_consumer_group}-watermark",
                "enable.auto.commit": False,
            }
        )
        try:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(self.context.ownership.names.kafka_topic, 0),
                timeout=10,
                cached=False,
            )
            return high
        finally:
            consumer.close()

    def _retire_other_pending_outbox_events(self, target_event_id: UUID) -> None:
        """Keep the publisher race scoped to the target row in the disposable database."""
        with self.factory() as session, session.begin():
            pending = session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.event_id != target_event_id,
                    OutboxEvent.published_at.is_(None),
                )
            ).all()
            retired_at = datetime.now(UTC)
            for event in pending:
                event.published_at = retired_at

    def _invariants_preserved(self) -> bool:
        with self.factory() as session:
            projections = session.scalars(select(OrderProjection)).all()
            for projection in projections:
                items = projection.payload.get("items", [])
                computed = Decimal("0")
                for item in items:
                    line_total = Decimal(str(item["line_total"]))
                    if line_total != Decimal(str(item["unit_price"])) * int(item["quantity"]):
                        return False
                    computed += line_total
                if computed != projection.total:
                    return False
                marker = session.get(ProcessedEvent, projection.source_event_id)
                if marker is None or marker.aggregate_id != projection.order_id:
                    return False
        return True

    def _cardinalities(self, fixture: RecoveryBenchmarkFixture) -> tuple[int, int, int]:
        with self.factory() as session:
            processed = session.scalar(
                select(func.count())
                .select_from(ProcessedEvent)
                .where(ProcessedEvent.event_id == fixture.event_id)
            )
            projection = session.scalar(
                select(func.count())
                .select_from(OrderProjection)
                .where(OrderProjection.order_id == fixture.order_id)
            )
            source_projection = session.scalar(
                select(func.count())
                .select_from(OrderProjection)
                .where(OrderProjection.source_event_id == fixture.event_id)
            )
        return processed, projection, source_projection

    def _audit_terminal(
        self,
        plan_id: UUID,
        *,
        lifecycle: RecoveryLifecycleState,
        execution_id: UUID | None = None,
        execution_status: RecoveryExecutionStatus | None = None,
    ) -> bool:
        with self.factory() as session:
            lifecycle_states = session.scalars(
                select(RecoveryLifecycleEvent.to_state)
                .where(RecoveryLifecycleEvent.plan_id == plan_id)
                .order_by(RecoveryLifecycleEvent.sequence_number)
            ).all()
            if not lifecycle_states or lifecycle_states[-1] != lifecycle.value:
                return False
            if execution_id is None:
                return True
            execution_states = session.scalars(
                select(RecoveryExecutionEvent.status)
                .where(RecoveryExecutionEvent.execution_id == execution_id)
                .order_by(RecoveryExecutionEvent.sequence_number)
            ).all()
            return bool(execution_states) and execution_states[-1] == execution_status.value

    def _assertions(
        self,
        *,
        fixture: RecoveryBenchmarkFixture,
        before,
        after,
        expected_write_set: set[str],
        expected_processed: int,
        expected_projection: int,
        outbox_valid: bool,
        kafka_before: int,
        kafka_after: int,
        audit_consistent: bool,
        mutation: bool,
        concurrency_safe: bool | None = None,
    ) -> AssertionResults:
        changed = recovery_verifier._changed_rows(before, after)
        processed, projection, source_projection = self._cardinalities(fixture)
        return AssertionResults(
            invariants_preserved=self._invariants_preserved(),
            declared_write_set_only=changed == expected_write_set,
            processed_event_cardinality_valid=processed == expected_processed,
            projection_cardinality_valid=(
                projection == expected_projection and source_projection == expected_projection
            ),
            outbox_effect_valid=outbox_valid,
            duplicate_business_effects=max(0, processed - 1)
            + max(0, projection - 1)
            + max(0, source_projection - 1),
            unexpected_kafka_publications=max(0, kafka_after - kafka_before),
            audit_consistent=audit_consistent,
            concurrency_safe=concurrency_safe,
            business_mutation_applied=mutation,
        )

    @staticmethod
    def _assertions_are_safe(assertions: AssertionResults) -> bool:
        return all(
            (
                assertions.invariants_preserved,
                assertions.declared_write_set_only,
                assertions.processed_event_cardinality_valid,
                assertions.projection_cardinality_valid,
                assertions.outbox_effect_valid,
                assertions.duplicate_business_effects == 0,
                assertions.unexpected_kafka_publications == 0,
                assertions.audit_consistent,
                assertions.concurrency_safe is not False,
            )
        )

    def _record(
        self,
        *,
        scenario_id: str,
        expected: BenchmarkOutcome,
        evidence_class: EvidenceClass,
        operation,
        trial: int = 1,
        concurrency: bool = False,
        scenario_family: ScenarioFamily | None = None,
        variation: str | None = None,
        trial_seed: int | None = None,
    ) -> ScenarioResult:
        self.trace = ScenarioTraceState()
        started = time.monotonic()
        previous_random = self.random
        if trial_seed is not None:
            self.random = random.Random(trial_seed)
        try:
            try:
                observation: ScenarioObservation = operation()
            except Exception as error:
                observation = ScenarioObservation(
                    actual_outcome=BenchmarkOutcome.OPERATIONAL_FAILURE,
                    assertions=AssertionResults(),
                    error=SanitizedError(
                        category="UNEXPECTED_SCENARIO_ERROR",
                        stage=BenchmarkStage.END_TO_END,
                        retryable=False,
                        message=f"scenario failed with {type(error).__name__}",
                    ),
                    limitations=["scenario did not produce conclusive safety assertions"],
                )
        finally:
            self.random = previous_random
        observation.timings.append(
            StageTiming(
                stage=BenchmarkStage.END_TO_END,
                duration_ms=(time.monotonic() - started) * 1000,
            )
        )
        if (
            observation.actual_outcome != BenchmarkOutcome.OPERATIONAL_FAILURE
            and not self._assertions_are_safe(observation.assertions)
        ):
            observation.actual_outcome = BenchmarkOutcome.OPERATIONAL_FAILURE
            observation.error = SanitizedError(
                category="BENCHMARK_ASSERTION_FAILED",
                stage=BenchmarkStage.FINAL_STATE_VALIDATION,
                retryable=False,
                message="independent safety assertion failed",
            )
        audit_trace = None
        if self.trace.incident_id is not None:
            with self.factory() as session:
                audit_trace = load_recovery_audit_trace(
                    session,
                    incident_id=self.trace.incident_id,
                    plan_id=self.trace.plan_id,
                    execution_id=self.trace.execution_id,
                    additional_reason_codes=self.trace.additional_reason_codes,
                )
        return ScenarioResult(
            identity=ScenarioIdentity(
                scenario_id=scenario_id,
                trial=trial,
                evidence_class=evidence_class,
                expected_outcome=expected,
                concurrency=concurrency,
                scenario_family=scenario_family,
                variation=variation,
                trial_seed=trial_seed,
            ),
            actual_outcome=observation.actual_outcome,
            assertions=observation.assertions,
            timings=tuple(observation.timings),
            error=observation.error,
            audit_trace=audit_trace,
            limitations=tuple(observation.limitations),
        )

    def _approved_replay(self, *, already_processed: bool = False) -> ScenarioObservation:
        timings: list[StageTiming] = []
        fixture = self._create_source(source="outbox", scenario=str(self._uuid()))
        if already_processed:
            with self.factory() as session:
                apply_event(session, parse_event(fixture.raw))
        plan = self._timed(
            BenchmarkStage.PLAN_CREATION,
            lambda: self._persist_plan(fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT),
            timings,
        )
        self._verify(plan, timings)
        self._approve(plan, timings)
        before = self._snapshot()
        kafka_before = self._watermark()
        result, execution_id = self._execute(plan, timings)
        kafka_after = self._watermark()
        after = self._snapshot()
        no_op = already_processed
        expected_write_set = (
            set()
            if no_op
            else {
                f"processed_events:{fixture.event_id}",
                f"order_projections:{fixture.order_id}",
            }
        )
        assertions = self._assertions(
            fixture=fixture,
            before=before,
            after=after,
            expected_write_set=expected_write_set,
            expected_processed=1,
            expected_projection=1,
            outbox_valid=True,
            kafka_before=kafka_before,
            kafka_after=kafka_after,
            audit_consistent=self._audit_terminal(
                plan.plan_id,
                lifecycle=(
                    RecoveryLifecycleState.NO_OP if no_op else RecoveryLifecycleState.SUCCEEDED
                ),
                execution_id=execution_id,
                execution_status=(
                    RecoveryExecutionStatus.NO_OP if no_op else RecoveryExecutionStatus.SUCCEEDED
                ),
            ),
            mutation=not no_op,
        )
        expected_status = (
            RecoveryExecutionStatus.NO_OP if no_op else RecoveryExecutionStatus.SUCCEEDED
        )
        return ScenarioObservation(
            actual_outcome=(
                BenchmarkOutcome.VERIFIED_NO_OP if no_op else BenchmarkOutcome.RECOVERED
            ),
            assertions=assertions,
            timings=timings,
            limitations=([] if result.status == expected_status else ["execution status mismatch"]),
        )

    def _outbox_reschedule(self) -> ScenarioObservation:
        timings: list[StageTiming] = []
        fixture = self._create_source(source="outbox", scenario=str(self._uuid()))
        plan = self._timed(
            BenchmarkStage.PLAN_CREATION,
            lambda: self._persist_plan(fixture, RecoveryActionType.RESCHEDULE_OUTBOX_EVENT),
            timings,
        )
        self._verify(plan, timings)
        self._approve(plan, timings)
        before = self._snapshot()
        kafka_before = self._watermark()
        result, execution_id = self._execute(plan, timings)
        kafka_after = self._watermark()
        after = self._snapshot()
        with self.factory() as session:
            outbox = session.get(OutboxEvent, fixture.event_id)
            outbox_valid = (
                outbox.published_at is None and outbox.next_attempt_at == outbox.occurred_at
            )
        assertions = self._assertions(
            fixture=fixture,
            before=before,
            after=after,
            expected_write_set={f"outbox_events:{fixture.event_id}"},
            expected_processed=0,
            expected_projection=0,
            outbox_valid=outbox_valid,
            kafka_before=kafka_before,
            kafka_after=kafka_after,
            audit_consistent=self._audit_terminal(
                plan.plan_id,
                lifecycle=RecoveryLifecycleState.SUCCEEDED,
                execution_id=execution_id,
                execution_status=RecoveryExecutionStatus.SUCCEEDED,
            ),
            mutation=True,
        )
        return ScenarioObservation(
            actual_outcome=BenchmarkOutcome.RECOVERED,
            assertions=assertions,
            timings=timings,
            limitations=([] if result.status == RecoveryExecutionStatus.SUCCEEDED else ["status"]),
        )

    def _unsafe_payload(self, kind: str) -> ScenarioObservation:
        timings: list[StageTiming] = []
        event_id, order_id = self._uuid(), self._uuid()
        if kind == "unsupported-schema":
            raw = self._raw(self._event_payload(event_id, order_id, schema_version=99))
        elif kind == "malformed-event":
            raw = b"not-json"
        else:
            raw = self._raw(self._event_payload(event_id, order_id, total="50.00"))
        fixture = self._create_source(
            source="quarantine",
            scenario=f"{kind}-{event_id}",
            raw=raw,
            event_id=event_id,
            order_id=order_id,
        )
        plan = self._timed(
            BenchmarkStage.PLAN_CREATION,
            lambda: self._persist_plan(fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT),
            timings,
        )
        before = self._snapshot()
        kafka_before = self._watermark()
        verification = self._verify(plan, timings)
        kafka_after = self._watermark()
        after = self._snapshot()
        assertions = self._assertions(
            fixture=fixture,
            before=before,
            after=after,
            expected_write_set=set(),
            expected_processed=0,
            expected_projection=0,
            outbox_valid=True,
            kafka_before=kafka_before,
            kafka_after=kafka_after,
            audit_consistent=(
                verification.decision.status == EligibilityStatus.INELIGIBLE
                and self._audit_terminal(plan.plan_id, lifecycle=RecoveryLifecycleState.REJECTED)
            ),
            mutation=False,
        )
        return ScenarioObservation(
            actual_outcome=BenchmarkOutcome.SAFELY_REJECTED,
            assertions=assertions,
            timings=timings,
        )

    def _evidence_rejection(self, *, foreign: bool) -> ScenarioObservation:
        timings: list[StageTiming] = []
        own = self._create_source(source="quarantine", scenario=f"evidence-own-{self._uuid()}")
        evidence_id = self._uuid()
        state_evidence = ()
        if foreign:
            other = self._create_source(
                source="quarantine", scenario=f"evidence-foreign-{self._uuid()}"
            )
            evidence_id = other.evidence_id
            with self.factory() as session:
                state_evidence = (
                    recovery_evidence_from_row(session.get(IncidentEvidence, evidence_id)),
                )
        plan = build_recovery_plan(
            action=RecoveryActionType.RETAIN_QUARANTINE,
            incident_id=own.incident_id,
            target=self._target(own, RecoveryActionType.RETAIN_QUARANTINE),
            evidence_ids=[evidence_id],
            state=RecoveryStateSnapshot(
                incident_id=own.incident_id,
                evidence=state_evidence,
            ),
        )
        self.trace.incident_id = own.incident_id
        self.trace.plan_id = plan.plan_id
        self.trace.additional_reason_codes = (
            "FOREIGN_EVIDENCE" if foreign else "MISSING_EVIDENCE",
        )
        before = self._snapshot()
        kafka_before = self._watermark()

        def persist() -> None:
            with self.factory() as session, session.begin():
                create_recovery_plan(
                    session,
                    plan,
                    source=RecoveryPlanSource.SYSTEM,
                    created_by="isolated-benchmark",
                )

        rejected = False
        try:
            self._timed(BenchmarkStage.PLAN_CREATION, persist, timings)
        except RecoveryEvidenceOwnershipError:
            rejected = True
        after = self._snapshot()
        kafka_after = self._watermark()
        with self.factory() as session:
            no_plan = session.get(RecoveryPlanEntity, plan.plan_id) is None
        assertions = self._assertions(
            fixture=own,
            before=before,
            after=after,
            expected_write_set=set(),
            expected_processed=0,
            expected_projection=0,
            outbox_valid=True,
            kafka_before=kafka_before,
            kafka_after=kafka_after,
            audit_consistent=rejected and no_plan,
            mutation=False,
        )
        return ScenarioObservation(
            actual_outcome=BenchmarkOutcome.SAFELY_REJECTED,
            assertions=assertions,
            timings=timings,
            limitations=["rejected before plan persistence; no lifecycle audit was created"],
        )

    def _stale_or_expired(self, *, expired: bool) -> ScenarioObservation:
        timings: list[StageTiming] = []
        fixture = self._create_source(source="outbox", scenario=str(self._uuid()))
        plan = self._persist_plan(fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
        verification = self._verify(plan, timings)
        execution_id = None
        if expired:
            self.trace.additional_reason_codes = ("APPROVAL_EXPIRED",)
            approval = self._approve(
                plan,
                timings,
                now=verification.persisted_verification.verified_at + timedelta(seconds=1),
            )
            before = self._snapshot()
            kafka_before = self._watermark()
            execution_id = self._uuid()
            rejected = False
            try:
                self._execute(
                    plan,
                    timings,
                    execution_id=execution_id,
                    now=approval.expires_at,
                )
            except RecoveryExecutionRejectedError:
                rejected = True
            expected_lifecycle = RecoveryLifecycleState.APPROVED
        else:
            self.trace.additional_reason_codes = ("STALE_STATE",)
            with self.factory() as session, session.begin():
                session.get(OutboxEvent, fixture.event_id).attempts += 1
            before = self._snapshot()
            kafka_before = self._watermark()
            rejected = False
            try:
                self._approve(plan, timings)
            except RecoveryApprovalStaleError:
                rejected = True
            expected_lifecycle = RecoveryLifecycleState.EXPIRED
        kafka_after = self._watermark()
        after = self._snapshot()
        audit = self._audit_terminal(plan.plan_id, lifecycle=expected_lifecycle)
        if expired and execution_id is not None:
            with self.factory() as session:
                states = session.scalars(
                    select(RecoveryExecutionEvent.status)
                    .where(RecoveryExecutionEvent.execution_id == execution_id)
                    .order_by(RecoveryExecutionEvent.sequence_number)
                ).all()
                audit = (
                    audit and bool(states) and states[-1] == RecoveryExecutionStatus.FAILED.value
                )
        assertions = self._assertions(
            fixture=fixture,
            before=before,
            after=after,
            expected_write_set=set(),
            expected_processed=0,
            expected_projection=0,
            outbox_valid=True,
            kafka_before=kafka_before,
            kafka_after=kafka_after,
            audit_consistent=rejected and audit,
            mutation=False,
        )
        return ScenarioObservation(
            actual_outcome=BenchmarkOutcome.SAFELY_REJECTED,
            assertions=assertions,
            timings=timings,
        )

    def _rollback_failure(self) -> ScenarioObservation:
        timings: list[StageTiming] = []
        fixture = self._create_source(source="outbox", scenario=str(self._uuid()))
        plan = self._persist_plan(fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
        self._verify(plan, timings)
        self._approve(plan, timings)
        before = self._snapshot()
        kafka_before = self._watermark()
        execution_id = self._uuid()
        original = recovery_verifier._simulate_eligible_action

        def apply_then_fail(session, selected_plan, state, decision):
            original(session, selected_plan, state, decision)
            raise RuntimeError("controlled benchmark rollback")

        recovery_verifier._simulate_eligible_action = apply_then_fail
        failed = False
        try:
            self._execute(plan, timings, execution_id=execution_id)
        except RuntimeError:
            failed = True
        finally:
            recovery_verifier._simulate_eligible_action = original
        kafka_after = self._watermark()
        after = self._snapshot()
        assertions = self._assertions(
            fixture=fixture,
            before=before,
            after=after,
            expected_write_set=set(),
            expected_processed=0,
            expected_projection=0,
            outbox_valid=True,
            kafka_before=kafka_before,
            kafka_after=kafka_after,
            audit_consistent=(
                failed
                and self._audit_terminal(plan.plan_id, lifecycle=RecoveryLifecycleState.APPROVED)
                and self._execution_failed(execution_id)
            ),
            mutation=False,
        )
        return ScenarioObservation(
            actual_outcome=BenchmarkOutcome.OPERATIONAL_FAILURE,
            assertions=assertions,
            timings=timings,
            error=SanitizedError(
                category="CONTROLLED_TRANSACTION_ROLLBACK",
                stage=BenchmarkStage.EXECUTION,
                retryable=False,
                message="controlled failure rolled back before commit",
            ),
            limitations=["failure was injected in-process; no process was killed"],
        )

    def _execution_failed(self, execution_id: UUID) -> bool:
        with self.factory() as session:
            states = session.scalars(
                select(RecoveryExecutionEvent.status)
                .where(RecoveryExecutionEvent.execution_id == execution_id)
                .order_by(RecoveryExecutionEvent.sequence_number)
            ).all()
        return bool(states) and states[-1] == RecoveryExecutionStatus.FAILED.value

    def _two_executors(self) -> ScenarioObservation:
        timings: list[StageTiming] = []
        fixture = self._create_source(source="outbox", scenario=str(self._uuid()))
        plan = self._persist_plan(fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
        self._verify(plan, timings)
        self._approve(plan, timings)
        before = self._snapshot()
        kafka_before = self._watermark()

        def attempt():
            execution_id = self._uuid()
            try:
                return (
                    execution_id,
                    execute_recovery_plan(
                        self.factory,
                        plan.plan_id,
                        execution_id,
                        expected_plan_hash=plan.plan_hash,
                        authorizer=self.authorizer,
                    ),
                )
            except RecoveryExecutionError as error:
                return execution_id, error

        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(lambda _item: attempt(), range(2)))
        timings.append(
            StageTiming(
                stage=BenchmarkStage.EXECUTION,
                duration_ms=(time.monotonic() - started) * 1000,
            )
        )
        kafka_after = self._watermark()
        after = self._snapshot()
        successes = [
            item
            for item in outcomes
            if not isinstance(item[1], Exception)
            and item[1].status == RecoveryExecutionStatus.SUCCEEDED
        ]
        if successes:
            self.trace.execution_id = successes[0][0]
        assertions = self._assertions(
            fixture=fixture,
            before=before,
            after=after,
            expected_write_set={
                f"processed_events:{fixture.event_id}",
                f"order_projections:{fixture.order_id}",
            },
            expected_processed=1,
            expected_projection=1,
            outbox_valid=True,
            kafka_before=kafka_before,
            kafka_after=kafka_after,
            audit_consistent=len(successes) == 1,
            mutation=True,
            concurrency_safe=len(successes) == 1,
        )
        return ScenarioObservation(
            actual_outcome=BenchmarkOutcome.RECOVERED,
            assertions=assertions,
            timings=timings,
            limitations=["concurrency used coordinated in-process threads with real transactions"],
        )

    def _consumer_executor(self) -> ScenarioObservation:
        timings: list[StageTiming] = []
        fixture = self._create_source(source="outbox", scenario=str(self._uuid()))
        plan = self._persist_plan(fixture, RecoveryActionType.REPLAY_VERIFIED_EVENT)
        self._verify(plan, timings)
        self._approve(plan, timings)
        before = self._snapshot()
        kafka_before = self._watermark()
        ready, release = Event(), Event()
        original_lock = recovery_verifier._acquire_target_locks

        def pause_after_lock(session, selected_plan):
            original_lock(session, selected_plan)
            ready.set()
            if not release.wait(10):
                raise TimeoutError("consumer race barrier timed out")

        recovery_verifier._acquire_target_locks = pause_after_lock
        execution_id = self._uuid()
        self.trace.execution_id = execution_id
        executor_error = None
        try:
            started = time.monotonic()
            with ThreadPoolExecutor(max_workers=2) as pool:
                execution = pool.submit(
                    execute_recovery_plan,
                    self.factory,
                    plan.plan_id,
                    execution_id,
                    expected_plan_hash=plan.plan_hash,
                    authorizer=self.authorizer,
                )
                if not ready.wait(10):
                    raise TimeoutError("executor did not reach race barrier")

                def consume():
                    with self.factory() as session:
                        return apply_event(session, parse_event(fixture.raw))

                consumed = pool.submit(consume).result(timeout=10)
                release.set()
                try:
                    execution.result(timeout=10)
                except RecoveryExecutionError as error:
                    executor_error = error
            timings.append(
                StageTiming(
                    stage=BenchmarkStage.EXECUTION,
                    duration_ms=(time.monotonic() - started) * 1000,
                )
            )
        finally:
            release.set()
            recovery_verifier._acquire_target_locks = original_lock
        kafka_after = self._watermark()
        after = self._snapshot()
        safe = consumed is True and isinstance(executor_error, RecoveryExecutionConflictError)
        assertions = self._assertions(
            fixture=fixture,
            before=before,
            after=after,
            expected_write_set={
                f"processed_events:{fixture.event_id}",
                f"order_projections:{fixture.order_id}",
            },
            expected_processed=1,
            expected_projection=1,
            outbox_valid=True,
            kafka_before=kafka_before,
            kafka_after=kafka_after,
            audit_consistent=safe and self._execution_failed(execution_id),
            mutation=True,
            concurrency_safe=safe,
        )
        return ScenarioObservation(
            actual_outcome=BenchmarkOutcome.OPERATIONAL_FAILURE,
            assertions=assertions,
            timings=timings,
            error=SanitizedError(
                category="CONCURRENT_CONSUMER_WON",
                stage=BenchmarkStage.EXECUTION,
                retryable=False,
                message="consumer committed the only business effect",
            ),
            limitations=["concurrency used coordinated in-process threads with real transactions"],
        )

    def _publisher_executor(self) -> ScenarioObservation:
        timings: list[StageTiming] = []
        fixture = self._create_source(source="outbox", scenario=str(self._uuid()))
        self._retire_other_pending_outbox_events(fixture.event_id)
        plan = self._persist_plan(fixture, RecoveryActionType.RESCHEDULE_OUTBOX_EVENT)
        self._verify(plan, timings)
        self._approve(plan, timings)
        before = self._snapshot()
        kafka_before = self._watermark()
        ready, release = Event(), Event()
        original_load = recovery_verifier.load_authoritative_recovery_snapshot
        hold = {"enabled": True}

        def pause_with_locked_outbox(session: Session, selected_plan):
            state = original_load(session, selected_plan)
            if hold["enabled"]:
                hold["enabled"] = False
                ready.set()
                if not release.wait(10):
                    raise TimeoutError("publisher race barrier timed out")
            return state

        recovery_verifier.load_authoritative_recovery_snapshot = pause_with_locked_outbox
        execution_id = self._uuid()
        self.trace.execution_id = execution_id
        try:
            started = time.monotonic()
            with ThreadPoolExecutor(max_workers=2) as pool:
                execution = pool.submit(
                    execute_recovery_plan,
                    self.factory,
                    plan.plan_id,
                    execution_id,
                    expected_plan_hash=plan.plan_hash,
                    authorizer=self.authorizer,
                )
                if not ready.wait(10):
                    raise TimeoutError("executor did not lock outbox row")
                published = pool.submit(
                    publish_pending,
                    self.factory,
                    KafkaEventPublisher(self.context.kafka_bootstrap_servers),
                    self.context.ownership.names.kafka_topic,
                ).result(timeout=10)
                release.set()
                result = execution.result(timeout=10)
            timings.append(
                StageTiming(
                    stage=BenchmarkStage.EXECUTION,
                    duration_ms=(time.monotonic() - started) * 1000,
                )
            )
        finally:
            release.set()
            recovery_verifier.load_authoritative_recovery_snapshot = original_load
        kafka_after = self._watermark()
        after = self._snapshot()
        with self.factory() as session:
            outbox = session.get(OutboxEvent, fixture.event_id)
            outbox_valid = (
                published == 0
                and outbox.published_at is None
                and outbox.next_attempt_at == outbox.occurred_at
            )
        safe = result.status == RecoveryExecutionStatus.SUCCEEDED and published == 0
        assertions = self._assertions(
            fixture=fixture,
            before=before,
            after=after,
            expected_write_set={f"outbox_events:{fixture.event_id}"},
            expected_processed=0,
            expected_projection=0,
            outbox_valid=outbox_valid,
            kafka_before=kafka_before,
            kafka_after=kafka_after,
            audit_consistent=safe
            and self._audit_terminal(
                plan.plan_id,
                lifecycle=RecoveryLifecycleState.SUCCEEDED,
                execution_id=execution_id,
                execution_status=RecoveryExecutionStatus.SUCCEEDED,
            ),
            mutation=True,
            concurrency_safe=safe,
        )
        return ScenarioObservation(
            actual_outcome=BenchmarkOutcome.RECOVERED,
            assertions=assertions,
            timings=timings,
            limitations=["publisher used skip-locked behavior in a coordinated real transaction"],
        )

    def run(self) -> list[ScenarioResult]:
        results = [
            self._record(
                scenario_id="approved-eligible-event-replay",
                expected=BenchmarkOutcome.RECOVERED,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=self._approved_replay,
            ),
            self._record(
                scenario_id="approved-eligible-outbox-reschedule",
                expected=BenchmarkOutcome.RECOVERED,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=self._outbox_reschedule,
            ),
            self._record(
                scenario_id="verified-duplicate-event-no-op",
                expected=BenchmarkOutcome.VERIFIED_NO_OP,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=lambda: self._approved_replay(already_processed=True),
            ),
            self._record(
                scenario_id="unsupported-schema-rejection",
                expected=BenchmarkOutcome.SAFELY_REJECTED,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=lambda: self._unsafe_payload("unsupported-schema"),
            ),
            self._record(
                scenario_id="malformed-event-rejection",
                expected=BenchmarkOutcome.SAFELY_REJECTED,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=lambda: self._unsafe_payload("malformed-event"),
            ),
            self._record(
                scenario_id="invalid-financial-total-rejection",
                expected=BenchmarkOutcome.SAFELY_REJECTED,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=lambda: self._unsafe_payload("invalid-total"),
            ),
            self._record(
                scenario_id="missing-evidence-rejection",
                expected=BenchmarkOutcome.SAFELY_REJECTED,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=lambda: self._evidence_rejection(foreign=False),
            ),
            self._record(
                scenario_id="foreign-evidence-rejection",
                expected=BenchmarkOutcome.SAFELY_REJECTED,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=lambda: self._evidence_rejection(foreign=True),
            ),
            self._record(
                scenario_id="stale-approval-rejection",
                expected=BenchmarkOutcome.SAFELY_REJECTED,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=lambda: self._stale_or_expired(expired=False),
            ),
            self._record(
                scenario_id="expired-approval-rejection",
                expected=BenchmarkOutcome.SAFELY_REJECTED,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=lambda: self._stale_or_expired(expired=True),
            ),
            self._record(
                scenario_id="transaction-rollback-on-injected-failure",
                expected=BenchmarkOutcome.OPERATIONAL_FAILURE,
                evidence_class=EvidenceClass.DETERMINISTIC_SIMULATION,
                operation=self._rollback_failure,
            ),
        ]
        for trial in range(1, self.context.concurrency_trials + 1):
            results.extend(
                [
                    self._record(
                        scenario_id="two-executors",
                        trial=trial,
                        expected=BenchmarkOutcome.RECOVERED,
                        evidence_class=EvidenceClass.REAL_INTEGRATION,
                        operation=self._two_executors,
                        concurrency=True,
                    ),
                    self._record(
                        scenario_id="consumer-versus-executor",
                        trial=trial,
                        expected=BenchmarkOutcome.OPERATIONAL_FAILURE,
                        evidence_class=EvidenceClass.REAL_INTEGRATION,
                        operation=self._consumer_executor,
                        concurrency=True,
                    ),
                    self._record(
                        scenario_id="publisher-versus-executor",
                        trial=trial,
                        expected=BenchmarkOutcome.RECOVERED,
                        evidence_class=EvidenceClass.REAL_INTEGRATION,
                        operation=self._publisher_executor,
                        concurrency=True,
                    ),
                ]
            )
        return results

    def run_expanded(self) -> list[ScenarioResult]:
        operations = {
            "approved-eligible-event-replay": self._approved_replay,
            "approved-eligible-outbox-reschedule": self._outbox_reschedule,
            "verified-duplicate-event-no-op": lambda: self._approved_replay(already_processed=True),
            "unsupported-schema-rejection": lambda: self._unsafe_payload("unsupported-schema"),
            "malformed-event-rejection": lambda: self._unsafe_payload("malformed-event"),
            "invalid-financial-total-rejection": lambda: self._unsafe_payload("invalid-total"),
            "missing-evidence-rejection": lambda: self._evidence_rejection(foreign=False),
            "foreign-evidence-rejection": lambda: self._evidence_rejection(foreign=True),
            "stale-approval-rejection": lambda: self._stale_or_expired(expired=False),
            "expired-approval-rejection": lambda: self._stale_or_expired(expired=True),
            "two-executors": self._two_executors,
            "consumer-versus-executor": self._consumer_executor,
            "publisher-versus-executor": self._publisher_executor,
        }
        return [
            self._record(
                scenario_id=spec.scenario_id,
                trial=spec.trial,
                expected=spec.expected_outcome,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=operations[spec.variation],
                concurrency=spec.concurrency,
                scenario_family=spec.scenario_family,
                variation=spec.variation,
                trial_seed=spec.trial_seed,
            )
            for spec in expanded_recovery_scenario_schedule(
                seed=self.context.seed,
                family_trials=self.context.serial_trials,
                concurrency_trials=self.context.concurrency_trials,
            )
        ]


def run_recovery_safety_scenarios(
    context: BenchmarkScenarioContext,
) -> list[ScenarioResult]:
    suite = RecoverySafetyScenarioSuite(context)
    try:
        return suite.run()
    finally:
        suite.close()


def run_expanded_recovery_safety_scenarios(
    context: BenchmarkScenarioContext,
) -> list[ScenarioResult]:
    suite = RecoverySafetyScenarioSuite(context)
    try:
        return suite.run_expanded()
    finally:
        suite.close()
