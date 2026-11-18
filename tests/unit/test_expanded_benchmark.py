from collections import Counter
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from fluxguard.apps import benchmark as benchmark_cli
from fluxguard.packages.common.benchmark_metrics import build_benchmark_report
from fluxguard.packages.common.benchmark_observability import build_operational_summary
from fluxguard.packages.common.benchmark_scenarios import (
    expanded_recovery_scenario_schedule,
)
from fluxguard.packages.contracts.benchmarks import (
    AssertionResults,
    BenchmarkConfiguration,
    BenchmarkEnvironment,
    BenchmarkMode,
    BenchmarkOutcome,
    BenchmarkReport,
    BenchmarkResourceNames,
    BenchmarkRunMetadata,
    EvidenceClass,
    ScenarioFamily,
    ScenarioIdentity,
    ScenarioResult,
)

RUN_ID = UUID("12345678-1234-4234-8234-123456789abc")


def _assertions(outcome: BenchmarkOutcome, concurrency: bool) -> AssertionResults:
    return AssertionResults(
        invariants_preserved=True,
        declared_write_set_only=True,
        processed_event_cardinality_valid=True,
        projection_cardinality_valid=True,
        outbox_effect_valid=True,
        duplicate_business_effects=0,
        unexpected_kafka_publications=0,
        audit_consistent=True,
        concurrency_safe=True if concurrency else None,
        business_mutation_applied=outcome == BenchmarkOutcome.RECOVERED,
    )


def test_expanded_schedule_has_exact_target_counts_and_distinct_categories() -> None:
    schedule = expanded_recovery_scenario_schedule(
        seed=20261117,
        family_trials=30,
        concurrency_trials=10,
    )

    assert len(schedule) == 120
    assert Counter(item.scenario_family for item in schedule) == {
        ScenarioFamily.ELIGIBLE_RECOVERY: 30,
        ScenarioFamily.UNSAFE_REJECTION: 30,
        ScenarioFamily.DUPLICATE_NO_OP: 30,
        ScenarioFamily.CONCURRENCY_SAFETY: 30,
    }
    assert Counter(
        item.variation
        for item in schedule
        if item.scenario_family == ScenarioFamily.CONCURRENCY_SAFETY
    ) == {
        "two-executors": 10,
        "consumer-versus-executor": 10,
        "publisher-versus-executor": 10,
    }
    assert len({item.variation for item in schedule}) == 13
    assert len({item.trial_seed for item in schedule}) == 120
    assert schedule == expanded_recovery_scenario_schedule(
        seed=20261117,
        family_trials=30,
        concurrency_trials=10,
    )
    assert schedule != expanded_recovery_scenario_schedule(
        seed=20261118,
        family_trials=30,
        concurrency_trials=10,
    )


def test_expanded_report_groups_by_evidence_family_and_variation() -> None:
    schedule = expanded_recovery_scenario_schedule(
        seed=9,
        family_trials=7,
        concurrency_trials=2,
    )
    results = [
        ScenarioResult(
            identity=ScenarioIdentity(
                scenario_id=item.scenario_id,
                trial=item.trial,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                expected_outcome=item.expected_outcome,
                concurrency=item.concurrency,
                scenario_family=item.scenario_family,
                variation=item.variation,
                trial_seed=item.trial_seed,
            ),
            actual_outcome=item.expected_outcome,
            assertions=_assertions(item.expected_outcome, item.concurrency),
            error=(
                None
                if item.expected_outcome != BenchmarkOutcome.OPERATIONAL_FAILURE
                else {
                    "category": "EXPECTED_CONCURRENCY_CONFLICT",
                    "stage": "EXECUTION",
                    "retryable": False,
                    "message": "one transaction won the coordinated race",
                }
            ),
        )
        for item in schedule
    ]
    report = build_benchmark_report(
        run=BenchmarkRunMetadata(
            run_id=RUN_ID,
            started_at=datetime(2026, 1, 1, tzinfo=UTC),
            completed_at=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
            duration_ms=60_000,
        ),
        configuration=BenchmarkConfiguration(
            benchmark_name="expanded-recovery-safety",
            mode=BenchmarkMode.EXPANDED_RECOVERY_SAFETY,
            seed=9,
            serial_trials=7,
            concurrency_trials=2,
            resources=BenchmarkResourceNames(
                database_name=f"fluxguard_benchmark_{RUN_ID.hex}",
                kafka_topic=f"fluxguard-benchmark-{RUN_ID.hex}-orders",
                kafka_consumer_group=f"fluxguard-benchmark-{RUN_ID.hex}-consumer",
            ),
        ),
        environment=BenchmarkEnvironment(python_version="3.12"),
        results=results,
    )

    assert sum(group.trials for group in report.scenario_groups) == len(schedule)
    assert {
        (group.evidence_class, group.scenario_family, group.variation)
        for group in report.scenario_groups
    } == {
        (EvidenceClass.REAL_INTEGRATION, item.scenario_family, item.variation) for item in schedule
    }
    assert build_operational_summary(report).total_scenarios == len(schedule)


def test_current_readers_accept_pre_extension_schema_v2_reports() -> None:
    result = ScenarioResult(
        identity=ScenarioIdentity(
            scenario_id="approved-eligible-event-replay",
            trial=1,
            evidence_class=EvidenceClass.REAL_INTEGRATION,
            expected_outcome=BenchmarkOutcome.RECOVERED,
            scenario_family=ScenarioFamily.ELIGIBLE_RECOVERY,
            variation="approved-eligible-event-replay",
            trial_seed=7,
        ),
        actual_outcome=BenchmarkOutcome.RECOVERED,
        assertions=_assertions(BenchmarkOutcome.RECOVERED, False),
    )
    report = build_benchmark_report(
        run=BenchmarkRunMetadata(
            run_id=RUN_ID,
            started_at=datetime(2026, 1, 1, tzinfo=UTC),
            completed_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            duration_ms=1000,
        ),
        configuration=BenchmarkConfiguration(
            benchmark_name="legacy-v2-reader-test",
            mode=BenchmarkMode.RECOVERY_SAFETY,
            seed=7,
            serial_trials=1,
            concurrency_trials=1,
            resources=BenchmarkResourceNames(
                database_name=f"fluxguard_benchmark_{RUN_ID.hex}",
                kafka_topic=f"fluxguard-benchmark-{RUN_ID.hex}-orders",
                kafka_consumer_group=f"fluxguard-benchmark-{RUN_ID.hex}-consumer",
            ),
        ),
        environment=BenchmarkEnvironment(python_version="3.12"),
        results=[result],
    )
    legacy_payload = report.model_dump(mode="json")
    legacy_payload.pop("scenario_groups")
    identity = legacy_payload["results"][0]["identity"]
    identity.pop("scenario_family")
    identity.pop("variation")
    identity.pop("trial_seed")

    legacy_report = BenchmarkReport.model_validate(legacy_payload)

    assert legacy_report.schema_version == 2
    assert legacy_report.scenario_groups == ()
    assert legacy_report.results[0].identity.scenario_family is None
    assert build_operational_summary(legacy_report).total_scenarios == 1


def test_scenario_classification_is_all_or_nothing_and_concurrency_consistent() -> None:
    with pytest.raises(ValidationError, match="must be recorded together"):
        ScenarioIdentity(
            scenario_id="replay",
            trial=1,
            evidence_class=EvidenceClass.REAL_INTEGRATION,
            expected_outcome=BenchmarkOutcome.RECOVERED,
            scenario_family=ScenarioFamily.ELIGIBLE_RECOVERY,
        )
    with pytest.raises(ValidationError, match="must be marked concurrent"):
        ScenarioIdentity(
            scenario_id="two-executors",
            trial=1,
            evidence_class=EvidenceClass.REAL_INTEGRATION,
            expected_outcome=BenchmarkOutcome.RECOVERED,
            scenario_family=ScenarioFamily.CONCURRENCY_SAFETY,
            variation="two-executors",
            trial_seed=1,
        )


def test_cli_accepts_expanded_mode_family_alias_and_rejects_negative_seed() -> None:
    arguments = benchmark_cli._parser().parse_args(
        [
            "--mode",
            "expanded-recovery-safety",
            "--family-trials",
            "30",
            "--concurrency-trials",
            "10",
            "--seed",
            "20261117",
        ]
    )
    assert arguments.serial_trials == 30
    assert arguments.concurrency_trials == 10
    assert arguments.seed == 20261117
    with pytest.raises(SystemExit):
        benchmark_cli._parser().parse_args(["--mode", "expanded-recovery-safety", "--seed", "-1"])
