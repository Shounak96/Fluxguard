import os
import time
from pathlib import Path

import pytest

from fluxguard.packages.common.benchmark_runner import (
    IsolationSmokeRunner,
    KafkaBenchmarkResources,
    PostgresBenchmarkResources,
    collect_environment,
)
from fluxguard.packages.common.benchmark_scenarios import run_recovery_safety_scenarios
from fluxguard.packages.contracts.benchmarks import (
    BenchmarkMode,
    BenchmarkOutcome,
    EvidenceClass,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_BENCHMARK_INTEGRATION") != "1",
        reason="set RUN_BENCHMARK_INTEGRATION=1 with local PostgreSQL and Redpanda",
    ),
]


def test_real_recovery_safety_benchmark_is_bounded_isolated_and_cleaned_up() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    postgres = PostgresBenchmarkResources(
        os.environ["BENCHMARK_POSTGRES_ADMIN_URL"], repository_root
    )
    kafka = KafkaBenchmarkResources(os.environ["BENCHMARK_KAFKA_BOOTSTRAP_SERVERS"])
    runner = IsolationSmokeRunner(
        postgres=postgres,
        kafka=kafka,
        environment_collector=lambda revision: collect_environment(repository_root, revision),
        scenario_executor=run_recovery_safety_scenarios,
        mode=BenchmarkMode.RECOVERY_SAFETY,
        benchmark_name="milestone-5-recovery-safety",
    )

    report = runner.run(seed=1, serial_trials=1, concurrency_trials=3)

    assert len(report.results) == 20
    assert (
        sum(
            result.identity.evidence_class == EvidenceClass.REAL_INTEGRATION
            for result in report.results
        )
        == 19
    )
    assert (
        sum(
            result.identity.evidence_class == EvidenceClass.DETERMINISTIC_SIMULATION
            for result in report.results
        )
        == 1
    )
    assert all(
        result.actual_outcome == result.identity.expected_outcome for result in report.results
    )
    assert all(
        result.assertions.invariants_preserved is True
        and result.assertions.declared_write_set_only is True
        and result.assertions.duplicate_business_effects == 0
        and result.assertions.unexpected_kafka_publications == 0
        and result.assertions.audit_consistent is True
        for result in report.results
    )
    assert all(
        result.assertions.concurrency_safe is True
        for result in report.results
        if result.identity.concurrency
    )
    concurrency = [result for result in report.results if result.identity.concurrency]
    assert len(concurrency) == 9
    assert {
        result.identity.scenario_id: sum(
            candidate.identity.scenario_id == result.identity.scenario_id
            for candidate in concurrency
        )
        for result in concurrency
    } == {
        "two-executors": 3,
        "consumer-versus-executor": 3,
        "publisher-versus-executor": 3,
    }
    assert (
        sum(result.actual_outcome == BenchmarkOutcome.RECOVERED for result in report.results) == 8
    )
    assert (
        sum(result.actual_outcome == BenchmarkOutcome.VERIFIED_NO_OP for result in report.results)
        == 1
    )
    assert (
        sum(result.actual_outcome == BenchmarkOutcome.SAFELY_REJECTED for result in report.results)
        == 7
    )
    assert (
        sum(
            result.actual_outcome == BenchmarkOutcome.OPERATIONAL_FAILURE
            for result in report.results
        )
        == 4
    )
    assert all(result.timings for result in report.results)
    assert report.environment.migration_revision == "0006_recovery_execution"
    assert not postgres.database_exists(report.configuration.resources.database_name)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and kafka.topic_exists(
        report.configuration.resources.kafka_topic
    ):
        time.sleep(0.25)
    assert not kafka.topic_exists(report.configuration.resources.kafka_topic)
