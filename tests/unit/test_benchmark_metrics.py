from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from fluxguard.packages.common.benchmark_metrics import (
    benchmark_report_json,
    build_benchmark_report,
)
from fluxguard.packages.contracts.benchmarks import (
    AssertionResults,
    BenchmarkConfiguration,
    BenchmarkEnvironment,
    BenchmarkMode,
    BenchmarkOutcome,
    BenchmarkResourceNames,
    BenchmarkRunMetadata,
    BenchmarkStage,
    EvidenceClass,
    ResourceUsage,
    SanitizedError,
    ScenarioIdentity,
    ScenarioResult,
    StageTiming,
)

RUN_ID = UUID("12345678-1234-4234-8234-123456789abc")
RUN = BenchmarkRunMetadata(
    run_id=RUN_ID,
    started_at=datetime(2026, 1, 1, tzinfo=UTC),
    completed_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
    duration_ms=1000,
)
CONFIGURATION = BenchmarkConfiguration(
    benchmark_name="milestone-5",
    mode=BenchmarkMode.ISOLATION_SMOKE,
    seed=7,
    serial_trials=1,
    concurrency_trials=3,
    resources=BenchmarkResourceNames(
        database_name=f"fluxguard_benchmark_{RUN_ID.hex}",
        kafka_topic=f"fluxguard-benchmark-{RUN_ID.hex}-orders",
        kafka_consumer_group=f"fluxguard-benchmark-{RUN_ID.hex}-consumer",
    ),
)
ENVIRONMENT = BenchmarkEnvironment(
    git_sha="a" * 40,
    dirty_worktree=False,
    python_version="3.12.7",
    postgres_image="postgres:16.8-alpine",
    redpanda_image="redpandadata/redpanda:v24.3.7",
    migration_revision="0006_recovery_execution",
)


def assertions(*, mutation: bool, concurrency_safe: bool | None = None) -> AssertionResults:
    return AssertionResults(
        invariants_preserved=True,
        declared_write_set_only=True,
        processed_event_cardinality_valid=True,
        projection_cardinality_valid=True,
        outbox_effect_valid=True,
        duplicate_business_effects=0,
        unexpected_kafka_publications=0,
        audit_consistent=True,
        concurrency_safe=concurrency_safe,
        business_mutation_applied=mutation,
    )


def result(
    scenario_id: str,
    *,
    evidence_class: EvidenceClass = EvidenceClass.REAL_INTEGRATION,
    expected: BenchmarkOutcome = BenchmarkOutcome.RECOVERED,
    actual: BenchmarkOutcome = BenchmarkOutcome.RECOVERED,
    trial: int = 1,
    concurrency: bool = False,
    timings: tuple[StageTiming, ...] = (),
) -> ScenarioResult:
    if actual == BenchmarkOutcome.OPERATIONAL_FAILURE:
        assertion_result = AssertionResults()
        error = SanitizedError(
            category="INFRASTRUCTURE_UNAVAILABLE",
            stage=BenchmarkStage.EXECUTION,
            retryable=True,
            message="test dependency was unavailable",
        )
    else:
        assertion_result = assertions(
            mutation=actual == BenchmarkOutcome.RECOVERED,
            concurrency_safe=True if concurrency else None,
        )
        error = None
    return ScenarioResult(
        identity=ScenarioIdentity(
            scenario_id=scenario_id,
            trial=trial,
            evidence_class=evidence_class,
            expected_outcome=expected,
            concurrency=concurrency,
        ),
        actual_outcome=actual,
        assertions=assertion_result,
        timings=timings,
        error=error,
    )


def cohort(report, evidence_class: EvidenceClass):
    return next(item for item in report.cohorts if item.evidence_class == evidence_class)


def test_metric_denominators_keep_recovery_noop_and_rejection_separate() -> None:
    results = [
        result("replay"),
        result(
            "duplicate",
            expected=BenchmarkOutcome.VERIFIED_NO_OP,
            actual=BenchmarkOutcome.VERIFIED_NO_OP,
        ),
        result(
            "unsafe",
            expected=BenchmarkOutcome.SAFELY_REJECTED,
            actual=BenchmarkOutcome.SAFELY_REJECTED,
        ),
        result(
            "recovery-dependency-failed",
            expected=BenchmarkOutcome.RECOVERED,
            actual=BenchmarkOutcome.OPERATIONAL_FAILURE,
        ),
    ]
    metrics = cohort(
        build_benchmark_report(
            run=RUN,
            configuration=CONFIGURATION,
            environment=ENVIRONMENT,
            results=results,
        ),
        EvidenceClass.REAL_INTEGRATION,
    ).metrics

    assert metrics.safe_recovery_success.model_dump() == {
        "numerator": 1,
        "denominator": 1,
        "unavailable": 1,
        "rate_percent": 100.0,
    }
    assert metrics.verified_no_op_correctness.denominator == 1
    assert metrics.unsafe_action_rejection.denominator == 1
    assert metrics.operational_completion.model_dump() == {
        "numerator": 3,
        "denominator": 4,
        "unavailable": 0,
        "rate_percent": 75.0,
    }


def test_mixed_evidence_classes_are_never_pooled() -> None:
    report = build_benchmark_report(
        run=RUN,
        configuration=CONFIGURATION,
        environment=ENVIRONMENT,
        results=[
            result("real"),
            result(
                "simulation",
                evidence_class=EvidenceClass.DETERMINISTIC_SIMULATION,
                actual=BenchmarkOutcome.OPERATIONAL_FAILURE,
            ),
            result("fixture", evidence_class=EvidenceClass.DETERMINISTIC_FIXTURE),
            result("provider", evidence_class=EvidenceClass.LIVE_PROVIDER),
            result("historical", evidence_class=EvidenceClass.HISTORICAL),
        ],
    )

    assert [item.evidence_class for item in report.cohorts] == list(EvidenceClass)
    assert all(item.trials == 1 for item in report.cohorts)
    assert (
        cohort(report, EvidenceClass.REAL_INTEGRATION).metrics.safe_recovery_success.numerator == 1
    )
    assert (
        cohort(
            report, EvidenceClass.DETERMINISTIC_SIMULATION
        ).metrics.safe_recovery_success.denominator
        == 0
    )


def test_zero_denominators_have_null_rates() -> None:
    report = build_benchmark_report(
        run=RUN,
        configuration=CONFIGURATION,
        environment=BenchmarkEnvironment(),
        results=[],
    )
    for item in report.cohorts:
        assert item.metrics.safe_recovery_success.rate_percent is None
        assert item.metrics.verified_no_op_correctness.rate_percent is None
        assert item.metrics.unsafe_action_rejection.rate_percent is None
        assert item.metrics.operational_completion.rate_percent is None


def test_latency_uses_only_recorded_values_and_counts_missing_measurements() -> None:
    report = build_benchmark_report(
        run=RUN,
        configuration=CONFIGURATION,
        environment=ENVIRONMENT,
        results=[
            result(
                "one",
                timings=(
                    StageTiming(stage=BenchmarkStage.VERIFICATION, duration_ms=10),
                    StageTiming(stage=BenchmarkStage.EXECUTION, duration_ms=30),
                ),
            ),
            result(
                "two",
                timings=(StageTiming(stage=BenchmarkStage.VERIFICATION, duration_ms=20),),
            ),
            result("three"),
        ],
    )
    latency = {item.stage: item for item in cohort(report, EvidenceClass.REAL_INTEGRATION).latency}

    verification = latency[BenchmarkStage.VERIFICATION]
    assert verification.count == 2
    assert verification.unavailable == 1
    assert verification.median_ms == 15
    assert verification.p95_ms == 20
    execution = latency[BenchmarkStage.EXECUTION]
    assert execution.count == 1
    assert execution.unavailable == 2
    assert latency[BenchmarkStage.APPROVAL].count == 0
    assert latency[BenchmarkStage.APPROVAL].median_ms is None


@pytest.mark.parametrize(
    ("actual", "assertion_result"),
    [
        (BenchmarkOutcome.RECOVERED, assertions(mutation=False)),
        (BenchmarkOutcome.VERIFIED_NO_OP, assertions(mutation=True)),
        (
            BenchmarkOutcome.SAFELY_REJECTED,
            AssertionResults(
                invariants_preserved=False,
                declared_write_set_only=True,
                processed_event_cardinality_valid=True,
                projection_cardinality_valid=True,
                outbox_effect_valid=True,
                duplicate_business_effects=0,
                unexpected_kafka_publications=0,
                audit_consistent=True,
                business_mutation_applied=False,
            ),
        ),
    ],
)
def test_incorrect_outcome_classification_is_rejected(actual, assertion_result) -> None:
    with pytest.raises(ValidationError):
        ScenarioResult(
            identity=ScenarioIdentity(
                scenario_id="contradiction",
                trial=1,
                evidence_class=EvidenceClass.REAL_INTEGRATION,
                expected_outcome=actual,
            ),
            actual_outcome=actual,
            assertions=assertion_result,
        )


def test_operational_failure_preserves_missing_assertions_and_measurements() -> None:
    failed = result("failed", actual=BenchmarkOutcome.OPERATIONAL_FAILURE)
    report = build_benchmark_report(
        run=RUN,
        configuration=CONFIGURATION,
        environment=BenchmarkEnvironment(),
        results=[failed],
    )
    metrics = cohort(report, EvidenceClass.REAL_INTEGRATION).metrics

    assert metrics.safe_recovery_success.denominator == 0
    assert metrics.safe_recovery_success.unavailable == 1
    assert metrics.audit_consistency.denominator == 0
    assert metrics.audit_consistency.unavailable == 1


def test_usage_rejects_inconsistent_token_totals() -> None:
    with pytest.raises(ValidationError):
        ResourceUsage(
            model_id="fixture/free",
            llm_calls=1,
            prompt_tokens=10,
            completion_tokens=5,
            total_tokens=20,
            provider_cost_usd=Decimal("0"),
        )


def test_stable_json_serialization_is_independent_of_input_order() -> None:
    first = result(
        "b-case",
        timings=(
            StageTiming(stage=BenchmarkStage.EXECUTION, duration_ms=2),
            StageTiming(stage=BenchmarkStage.VERIFICATION, duration_ms=1),
        ),
    )
    second = result("a-case")
    report_one = build_benchmark_report(
        run=RUN,
        configuration=CONFIGURATION,
        environment=ENVIRONMENT,
        results=[first, second],
        limitations=["bounded local evidence", "single-node services"],
    )
    report_two = build_benchmark_report(
        run=RUN,
        configuration=CONFIGURATION,
        environment=ENVIRONMENT,
        results=[second, first],
        limitations=["single-node services", "bounded local evidence"],
    )

    assert benchmark_report_json(report_one) == benchmark_report_json(report_two)
    assert benchmark_report_json(report_one).startswith('{"cohorts":')
    assert '"schema_version":2' in benchmark_report_json(report_one)


@pytest.mark.parametrize(
    "payload",
    [
        {"database_url": "postgresql://user:password@host/database"},
        {"python_version": "password=do-not-store"},
    ],
)
def test_environment_rejects_sensitive_or_unknown_fields(payload) -> None:
    with pytest.raises(ValidationError):
        BenchmarkEnvironment.model_validate(payload)


def test_result_rejects_raw_payload_and_sensitive_error_message() -> None:
    base = {
        "identity": {
            "scenario_id": "unsafe-report",
            "trial": 1,
            "evidence_class": "REAL_INTEGRATION",
            "expected_outcome": "OPERATIONAL_FAILURE",
        },
        "actual_outcome": "OPERATIONAL_FAILURE",
        "assertions": {},
        "error": {
            "category": "PROVIDER_ERROR",
            "stage": "INVESTIGATION",
            "retryable": False,
            "message": "api_key=do-not-store",
        },
        "raw_event_payload": {"customer": "sensitive"},
    }
    with pytest.raises(ValidationError) as error:
        ScenarioResult.model_validate(base)
    locations = {tuple(item["loc"]) for item in error.value.errors()}
    assert ("raw_event_payload",) in locations
    assert ("error",) in locations
