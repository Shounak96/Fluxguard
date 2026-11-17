import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TypeVar
from uuid import UUID

from confluent_kafka import Consumer, Producer
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from fluxguard.apps.api.main import create_app
from fluxguard.apps.worker.consumer import EventRecord, process_record
from fluxguard.apps.worker.outbox import KafkaEventPublisher, event_bytes, publish_pending
from fluxguard.packages.common import recovery_verifier
from fluxguard.packages.common.benchmark_runner import BenchmarkScenarioContext
from fluxguard.packages.common.benchmark_scenarios import (
    RecoverySafetyScenarioSuite,
    ScenarioObservation,
)
from fluxguard.packages.common.config import Settings
from fluxguard.packages.common.models import (
    Incident,
    IncidentEvidence,
    Order,
    OrderProjection,
    OutboxEvent,
    ProcessedEvent,
    QuarantinedEvent,
)
from fluxguard.packages.common.recoveries import (
    create_recovery_plan,
    recovery_evidence_from_row,
)
from fluxguard.packages.common.recovery_approvals import (
    DisabledRecoveryAuthorizer,
    RecoveryAuthorizationError,
    approve_recovery_plan,
)
from fluxguard.packages.common.recovery_executor import execute_recovery_plan
from fluxguard.packages.common.recovery_policy import build_recovery_plan
from fluxguard.packages.common.time import utc_now
from fluxguard.packages.contracts.benchmarks import (
    AssertionResults,
    BenchmarkOutcome,
    BenchmarkStage,
    EvidenceClass,
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

T = TypeVar("T")


@dataclass
class DemoTimings:
    durations_ms: dict[BenchmarkStage, float] = field(default_factory=dict)

    def measure(self, stage: BenchmarkStage, operation: Callable[[], T]) -> T:
        started = time.monotonic()
        try:
            return operation()
        finally:
            self.durations_ms[stage] = (
                self.durations_ms.get(stage, 0.0) + (time.monotonic() - started) * 1000
            )

    def contracts(self) -> list[StageTiming]:
        return [
            StageTiming(stage=stage, duration_ms=duration)
            for stage, duration in sorted(self.durations_ms.items(), key=lambda item: item[0].value)
        ]


class ControlledPublicationFailure:
    def publish(self, _topic: str, _key: str, _value: bytes) -> None:
        raise ConnectionError("controlled isolated demo publication failure")


class EndToEndDemoSuite(RecoverySafetyScenarioSuite):
    def _api_order(self, idempotency_key: str, customer_id: str) -> UUID:
        def session_dependency():
            with self.factory() as session:
                yield session

        app = create_app(session_dependency)
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/orders",
                headers={"Idempotency-Key": idempotency_key},
                json={
                    "customer_id": customer_id,
                    "items": [
                        {
                            "product_id": "demo-product",
                            "quantity": 2,
                            "unit_price": "12.50",
                        }
                    ],
                    "currency": "USD",
                },
            )
        if response.status_code != 201:
            raise RuntimeError("isolated order API did not accept the demo order")
        return UUID(response.json()["data"]["order_id"])

    def _consume(self, order_id: UUID, *, group_suffix: str) -> EventRecord:
        group_id = f"{self.context.ownership.names.kafka_consumer_group}-{group_suffix}"
        consumer = Consumer(
            {
                "bootstrap.servers": self.context.kafka_bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([self.context.ownership.names.kafka_topic])
        deadline = time.monotonic() + 20
        try:
            while time.monotonic() < deadline:
                message = consumer.poll(1)
                if message is None or message.error():
                    continue
                self.context.register_kafka_group(group_id)
                if message.key() != str(order_id).encode():
                    consumer.commit(message=message, asynchronous=False)
                    continue
                record = EventRecord(
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    message.key(),
                    message.value(),
                )
                outcome = process_record(
                    self.factory,
                    record,
                    Settings(fault_injection_enabled=False),
                )
                consumer.commit(message=message, asynchronous=False)
                if outcome not in {"processed", "quarantined"}:
                    raise RuntimeError("demo consumer produced an unexpected outcome")
                return record
        finally:
            consumer.close()
        raise TimeoutError("demo consumer did not observe its owned event")

    def _outbox_for_order(self, order_id: UUID) -> OutboxEvent:
        with self.factory() as session:
            event = session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == order_id))
            if event is None:
                raise LookupError("demo order has no transactional outbox event")
            session.expunge(event)
            return event

    def _incident_evidence(
        self,
        *,
        error_code: str,
        event_id: UUID | None = None,
        coordinate: KafkaCoordinate | None = None,
    ) -> tuple[Incident, IncidentEvidence]:
        with self.factory() as session:
            statement = (
                select(Incident, IncidentEvidence)
                .join(IncidentEvidence)
                .where(Incident.error_code == error_code)
            )
            if event_id is not None:
                statement = statement.where(IncidentEvidence.event_id == event_id)
            if coordinate is not None:
                statement = statement.where(
                    IncidentEvidence.kafka_topic == coordinate.topic,
                    IncidentEvidence.kafka_partition == coordinate.partition,
                    IncidentEvidence.kafka_offset == coordinate.offset,
                )
            row = session.execute(statement).one_or_none()
            if row is None:
                raise LookupError("demo incident evidence was not persisted")
            incident, evidence = row
            session.expunge(incident)
            session.expunge(evidence)
            return incident, evidence

    def _persist_authoritative_plan(
        self,
        *,
        action: RecoveryActionType,
        incident: Incident,
        evidence: IncidentEvidence,
        target: RecoveryTarget,
        expected_event_sha256: str,
    ):
        evidence_contract = recovery_evidence_from_row(evidence)
        provisional = build_recovery_plan(
            action=action,
            incident_id=incident.incident_id,
            target=target,
            evidence_ids=[evidence.evidence_id],
            state=RecoveryStateSnapshot(
                incident_id=incident.incident_id,
                evidence=(evidence_contract,),
            ),
            expected_event_sha256=expected_event_sha256,
        )
        with self.factory() as session, session.begin():
            snapshot = recovery_verifier.load_authoritative_recovery_snapshot(session, provisional)
        plan = build_recovery_plan(
            action=action,
            incident_id=incident.incident_id,
            target=target,
            evidence_ids=[evidence.evidence_id],
            state=snapshot,
            expected_event_sha256=expected_event_sha256,
        )
        with self.factory() as session, session.begin():
            create_recovery_plan(
                session,
                plan,
                source=RecoveryPlanSource.SYSTEM,
                created_by="isolated-end-to-end-demo",
            )
        self.trace.incident_id = incident.incident_id
        self.trace.plan_id = plan.plan_id
        return plan

    def _cardinality(self, model, predicate) -> int:
        with self.factory() as session:
            return session.scalar(select(func.count()).select_from(model).where(predicate))

    def _recoverable_lane(self) -> ScenarioObservation:
        timings = DemoTimings()
        topic = self.context.ownership.names.kafka_topic
        initial_watermark = timings.measure(BenchmarkStage.KAFKA_WATERMARK, self._watermark)

        normal_order_id = timings.measure(
            BenchmarkStage.API_ORDER_SUBMISSION,
            lambda: self._api_order(
                f"demo-normal-{self.context.ownership.run_id}", "demo-normal-customer"
            ),
        )
        published = timings.measure(
            BenchmarkStage.OUTBOX_PUBLICATION,
            lambda: publish_pending(
                self.factory,
                KafkaEventPublisher(self.context.kafka_bootstrap_servers),
                topic,
            ),
        )
        normal_record = timings.measure(
            BenchmarkStage.CONSUMER_PROCESSING,
            lambda: self._consume(normal_order_id, group_suffix="normal"),
        )
        if published != 1:
            raise RuntimeError("normal demo outbox publication count was not one")

        fault_order_id = timings.measure(
            BenchmarkStage.API_ORDER_SUBMISSION,
            lambda: self._api_order(
                f"demo-fault-{self.context.ownership.run_id}", "demo-recovery-customer"
            ),
        )
        fault_outbox = self._outbox_for_order(fault_order_id)

        def inject_failures() -> None:
            for attempt in range(3):
                result = publish_pending(
                    self.factory,
                    ControlledPublicationFailure(),
                    topic,
                    batch_size=1,
                    failure_threshold=3,
                )
                if result != 0:
                    raise RuntimeError("controlled publication failure unexpectedly published")
                if attempt < 2:
                    with self.factory() as session, session.begin():
                        event = session.get(OutboxEvent, fault_outbox.event_id)
                        event.next_attempt_at = utc_now() - timedelta(seconds=1)

        timings.measure(BenchmarkStage.FAULT_INJECTION, inject_failures)
        incident, evidence = timings.measure(
            BenchmarkStage.INCIDENT_PERSISTENCE,
            lambda: self._incident_evidence(
                error_code="OUTBOX_RETRY_THRESHOLD_EXCEEDED",
                event_id=fault_outbox.event_id,
            ),
        )
        authoritative_hash = hashlib.sha256(event_bytes(fault_outbox)).hexdigest()
        if evidence.payload.get("raw_sha256") != authoritative_hash:
            raise RuntimeError("persisted outbox evidence does not bind exact event bytes")

        plan = timings.measure(
            BenchmarkStage.PLAN_CREATION,
            lambda: self._persist_authoritative_plan(
                action=RecoveryActionType.RESCHEDULE_OUTBOX_EVENT,
                incident=incident,
                evidence=evidence,
                target=RecoveryTarget(
                    event_id=fault_outbox.event_id,
                    aggregate_id=fault_order_id,
                    outbox_event_id=fault_outbox.event_id,
                ),
                expected_event_sha256=authoritative_hash,
            ),
        )
        dry_run = timings.measure(
            BenchmarkStage.VERIFICATION,
            lambda: recovery_verifier.verify_recovery_plan_dry_run(self.factory, plan.plan_id),
        )
        if dry_run.decision.status != EligibilityStatus.ELIGIBLE:
            raise RuntimeError("demo outbox recovery was not deterministically eligible")

        denied = False
        try:
            DisabledRecoveryAuthorizer().authenticate()
        except RecoveryAuthorizationError:
            denied = True
        if not denied:
            raise RuntimeError("default runtime recovery approval was not denied")

        def approve():
            with self.factory() as session:
                return approve_recovery_plan(
                    session,
                    plan.plan_id,
                    operator=self.authorizer.authorize(plan.plan_id, self._uuid()),
                    idempotency_key=f"demo-approval-{plan.plan_id}",
                )

        timings.measure(BenchmarkStage.APPROVAL, approve)
        before_execution = self._snapshot()
        execution_id = self._uuid()
        self.trace.execution_id = execution_id
        result = timings.measure(
            BenchmarkStage.EXECUTION,
            lambda: execute_recovery_plan(
                self.factory,
                plan.plan_id,
                execution_id,
                expected_plan_hash=plan.plan_hash,
                authorizer=self.authorizer,
            ),
        )
        after_execution = self._snapshot()
        declared_write_set_only = recovery_verifier._changed_rows(
            before_execution, after_execution
        ) == {f"outbox_events:{fault_outbox.event_id}"}

        before_recovered_publish = timings.measure(BenchmarkStage.KAFKA_WATERMARK, self._watermark)
        recovered_published = timings.measure(
            BenchmarkStage.OUTBOX_PUBLICATION,
            lambda: publish_pending(
                self.factory,
                KafkaEventPublisher(self.context.kafka_bootstrap_servers),
                topic,
                batch_size=1,
            ),
        )
        fault_record = timings.measure(
            BenchmarkStage.CONSUMER_PROCESSING,
            lambda: self._consume(fault_order_id, group_suffix="recovered"),
        )
        final_watermark = timings.measure(BenchmarkStage.KAFKA_WATERMARK, self._watermark)

        def validate_final() -> bool:
            with self.factory() as session:
                outbox = session.get(OutboxEvent, fault_outbox.event_id)
                normal_projection = session.get(OrderProjection, normal_order_id)
                fault_projection = session.get(OrderProjection, fault_order_id)
                return all(
                    (
                        outbox is not None and outbox.published_at is not None,
                        normal_projection is not None,
                        fault_projection is not None,
                        session.get(ProcessedEvent, normal_projection.source_event_id) is not None,
                        session.get(ProcessedEvent, fault_projection.source_event_id) is not None,
                        session.get(Order, normal_order_id) is not None,
                        session.get(Order, fault_order_id) is not None,
                    )
                )

        final_valid = timings.measure(BenchmarkStage.FINAL_STATE_VALIDATION, validate_final)
        processed_valid = all(
            (
                self._cardinality(
                    ProcessedEvent,
                    ProcessedEvent.event_id == UUID(json.loads(normal_record.value)["event_id"]),
                )
                == 1,
                self._cardinality(
                    ProcessedEvent,
                    ProcessedEvent.event_id == UUID(json.loads(fault_record.value)["event_id"]),
                )
                == 1,
            )
        )
        projection_valid = all(
            (
                self._cardinality(OrderProjection, OrderProjection.order_id == normal_order_id)
                == 1,
                self._cardinality(OrderProjection, OrderProjection.order_id == fault_order_id) == 1,
            )
        )
        expected_publications = final_watermark - initial_watermark == 2
        reschedule_publication = final_watermark - before_recovered_publish == 1
        audit_consistent = self._audit_terminal(
            plan.plan_id,
            lifecycle=RecoveryLifecycleState.SUCCEEDED,
            execution_id=execution_id,
            execution_status=RecoveryExecutionStatus.SUCCEEDED,
        )
        assertions = AssertionResults(
            invariants_preserved=self._invariants_preserved(),
            declared_write_set_only=declared_write_set_only,
            processed_event_cardinality_valid=processed_valid,
            projection_cardinality_valid=projection_valid,
            outbox_effect_valid=(
                final_valid
                and recovered_published == 1
                and result.status == RecoveryExecutionStatus.SUCCEEDED
                and reschedule_publication
            ),
            duplicate_business_effects=0 if processed_valid and projection_valid else 1,
            unexpected_kafka_publications=0 if expected_publications else 1,
            audit_consistent=audit_consistent,
            business_mutation_applied=True,
        )
        return ScenarioObservation(
            actual_outcome=BenchmarkOutcome.RECOVERED,
            assertions=assertions,
            timings=timings.contracts(),
            limitations=[
                "investigation was deterministic evidence inspection; no LLM was called",
                "default runtime approval was denied before test-only approval",
            ],
        )

    def _unsafe_lane(self) -> ScenarioObservation:
        timings = DemoTimings()
        topic = self.context.ownership.names.kafka_topic
        before_processed = self._cardinality(ProcessedEvent, True)
        before_projections = self._cardinality(OrderProjection, True)
        before_watermark = timings.measure(BenchmarkStage.KAFKA_WATERMARK, self._watermark)
        event_id, order_id = self._uuid(), self._uuid()
        raw = self._raw(self._event_payload(event_id, order_id, schema_version=99))

        def publish_unsafe() -> None:
            producer = Producer(
                {
                    "bootstrap.servers": self.context.kafka_bootstrap_servers,
                    "acks": "all",
                    "enable.idempotence": True,
                    "client.id": "fluxguard-isolated-demo",
                }
            )
            producer.produce(topic, key=str(order_id), value=raw)
            if producer.flush(10):
                raise TimeoutError("unsafe demo event was not acknowledged")

        timings.measure(BenchmarkStage.FAULT_INJECTION, publish_unsafe)
        record = timings.measure(
            BenchmarkStage.CONSUMER_PROCESSING,
            lambda: self._consume(order_id, group_suffix="unsafe"),
        )
        coordinate = KafkaCoordinate(
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
        )
        incident, evidence = timings.measure(
            BenchmarkStage.INCIDENT_PERSISTENCE,
            lambda: self._incident_evidence(
                error_code="UNSUPPORTED_SCHEMA_VERSION", coordinate=coordinate
            ),
        )
        event_hash = hashlib.sha256(raw).hexdigest()
        if evidence.payload.get("raw_sha256") != event_hash:
            raise RuntimeError("unsafe incident evidence does not bind exact event bytes")
        plan = timings.measure(
            BenchmarkStage.PLAN_CREATION,
            lambda: self._persist_authoritative_plan(
                action=RecoveryActionType.REPLAY_VERIFIED_EVENT,
                incident=incident,
                evidence=evidence,
                target=RecoveryTarget(
                    event_id=event_id,
                    aggregate_id=order_id,
                    kafka_coordinate=coordinate,
                ),
                expected_event_sha256=event_hash,
            ),
        )
        dry_run = timings.measure(
            BenchmarkStage.VERIFICATION,
            lambda: recovery_verifier.verify_recovery_plan_dry_run(self.factory, plan.plan_id),
        )
        after_watermark = timings.measure(BenchmarkStage.KAFKA_WATERMARK, self._watermark)

        def validate_final() -> bool:
            with self.factory() as session:
                quarantine = session.scalar(
                    select(QuarantinedEvent).where(
                        QuarantinedEvent.topic == coordinate.topic,
                        QuarantinedEvent.partition == coordinate.partition,
                        QuarantinedEvent.offset == coordinate.offset,
                    )
                )
                return all(
                    (
                        quarantine is not None,
                        session.get(ProcessedEvent, event_id) is None,
                        session.get(OrderProjection, order_id) is None,
                    )
                )

        final_valid = timings.measure(BenchmarkStage.FINAL_STATE_VALIDATION, validate_final)
        processed_unchanged = self._cardinality(ProcessedEvent, True) == before_processed
        projections_unchanged = self._cardinality(OrderProjection, True) == before_projections
        rejected = (
            dry_run.decision.status == EligibilityStatus.INELIGIBLE
            and "UNSUPPORTED_SCHEMA" in {reason.value for reason in dry_run.decision.reasons}
        )
        audit_consistent = self._audit_terminal(
            plan.plan_id, lifecycle=RecoveryLifecycleState.REJECTED
        )
        return ScenarioObservation(
            actual_outcome=BenchmarkOutcome.SAFELY_REJECTED,
            assertions=AssertionResults(
                invariants_preserved=self._invariants_preserved(),
                declared_write_set_only=processed_unchanged and projections_unchanged,
                processed_event_cardinality_valid=processed_unchanged,
                projection_cardinality_valid=projections_unchanged,
                outbox_effect_valid=True,
                duplicate_business_effects=0,
                unexpected_kafka_publications=(0 if after_watermark - before_watermark == 1 else 1),
                audit_consistent=rejected and audit_consistent and final_valid,
                business_mutation_applied=False,
            ),
            timings=timings.contracts(),
            limitations=["investigation was deterministic evidence inspection; no LLM was called"],
        )

    def run(self) -> list[ScenarioResult]:
        return [
            self._record(
                scenario_id="end-to-end-recoverable-outbox",
                expected=BenchmarkOutcome.RECOVERED,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=self._recoverable_lane,
            ),
            self._record(
                scenario_id="end-to-end-unsafe-schema",
                expected=BenchmarkOutcome.SAFELY_REJECTED,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                operation=self._unsafe_lane,
            ),
        ]


def run_end_to_end_demo(context: BenchmarkScenarioContext) -> list[ScenarioResult]:
    suite = EndToEndDemoSuite(context)
    try:
        return suite.run()
    finally:
        suite.close()
