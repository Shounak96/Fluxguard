import json
import os
import time
from pathlib import Path

import pytest

from fluxguard.packages.common.benchmark_demo import run_end_to_end_demo
from fluxguard.packages.common.benchmark_observability import build_operational_summary
from fluxguard.packages.common.benchmark_runner import (
    IsolationSmokeRunner,
    KafkaBenchmarkResources,
    PostgresBenchmarkResources,
    collect_environment,
)
from fluxguard.packages.contracts.benchmarks import (
    AuditReconciliationStatus,
    BenchmarkMode,
    BenchmarkOutcome,
    BenchmarkStage,
    EvidenceClass,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_DEMO_INTEGRATION") != "1",
        reason="set RUN_DEMO_INTEGRATION=1 with local PostgreSQL and Redpanda",
    ),
]


def test_real_end_to_end_demo_is_safe_reconciled_and_fully_cleaned() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    postgres = PostgresBenchmarkResources(
        os.environ["BENCHMARK_POSTGRES_ADMIN_URL"], repository_root
    )
    kafka = KafkaBenchmarkResources(os.environ["BENCHMARK_KAFKA_BOOTSTRAP_SERVERS"])
    runner = IsolationSmokeRunner(
        postgres=postgres,
        kafka=kafka,
        environment_collector=lambda revision: collect_environment(repository_root, revision),
        scenario_executor=run_end_to_end_demo,
        mode=BenchmarkMode.END_TO_END_DEMO,
        benchmark_name="milestone-5-end-to-end-demo",
    )

    report = runner.run(seed=1, serial_trials=1, concurrency_trials=3)
    summary = build_operational_summary(report)

    assert report.configuration.mode == BenchmarkMode.END_TO_END_DEMO
    assert report.environment.migration_revision == "0006_recovery_execution"
    assert len(report.results) == 2
    assert [result.actual_outcome for result in report.results] == [
        BenchmarkOutcome.RECOVERED,
        BenchmarkOutcome.SAFELY_REJECTED,
    ]
    assert all(
        result.identity.evidence_class == EvidenceClass.REAL_INTEGRATION
        and result.assertions.invariants_preserved is True
        and result.assertions.declared_write_set_only is True
        and result.assertions.processed_event_cardinality_valid is True
        and result.assertions.projection_cardinality_valid is True
        and result.assertions.outbox_effect_valid is True
        and result.assertions.duplicate_business_effects == 0
        and result.assertions.unexpected_kafka_publications == 0
        and result.assertions.audit_consistent is True
        for result in report.results
    )
    assert all(
        result.audit_trace is not None
        and result.audit_trace.incident_id is not None
        and result.audit_trace.plan_id is not None
        and result.audit_trace.verification_id is not None
        and result.audit_trace.reconciliation_status == AuditReconciliationStatus.CONSISTENT
        for result in report.results
    )
    assert report.results[0].audit_trace.approval_id is not None
    assert report.results[0].audit_trace.execution_id is not None
    assert report.results[1].audit_trace.approval_id is None
    assert report.results[1].audit_trace.execution_id is None
    for result in report.results:
        stages = {timing.stage for timing in result.timings}
        assert BenchmarkStage.KAFKA_WATERMARK in stages
        assert BenchmarkStage.FINAL_STATE_VALIDATION in stages
        assert BenchmarkStage.END_TO_END in stages

    real = next(
        cohort
        for cohort in summary.cohorts
        if cohort.evidence_class == EvidenceClass.REAL_INTEGRATION
    )
    assert real.outcomes.recovery_execution_success == 1
    assert real.outcomes.safe_rejection == 1
    assert real.outcomes.unexpected_operational_failure == 0
    assert real.outcomes.safety_assertion_failure == 0
    assert all(
        trace.reconciliation_status == AuditReconciliationStatus.CONSISTENT
        for trace in summary.traces
    )

    assert not postgres.database_exists(report.configuration.resources.database_name)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and kafka.topic_exists(
        report.configuration.resources.kafka_topic
    ):
        time.sleep(0.25)
    assert not kafka.topic_exists(report.configuration.resources.kafka_topic)
    assert not kafka.group_exists(f"{report.configuration.resources.kafka_consumer_group}-normal")
    assert not kafka.group_exists(
        f"{report.configuration.resources.kafka_consumer_group}-recovered"
    )
    assert not kafka.group_exists(f"{report.configuration.resources.kafka_consumer_group}-unsafe")

    latency = {
        item.stage.value: {
            "count": item.count,
            "median_ms": item.median_ms,
            "p95_ms": item.p95_ms,
        }
        for item in real.latency
        if item.count
    }
    print(
        "DEMO_OBSERVATION="
        + json.dumps(
            {
                "run_id": str(report.run.run_id),
                "outcomes": [result.actual_outcome.value for result in report.results],
                "latency": latency,
                "audit_reconciliation": [
                    trace.reconciliation_status.value for trace in summary.traces
                ],
            },
            sort_keys=True,
        )
    )
