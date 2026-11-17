import json
import math
import statistics
from collections.abc import Callable

from fluxguard.packages.contracts.benchmarks import (
    BenchmarkConfiguration,
    BenchmarkEnvironment,
    BenchmarkMetrics,
    BenchmarkOutcome,
    BenchmarkReport,
    BenchmarkRunMetadata,
    BenchmarkStage,
    EvidenceClass,
    EvidenceClassSummary,
    MetricRate,
    ScenarioResult,
    StageLatencySummary,
)


def _rate(numerator: int, denominator: int, unavailable: int) -> MetricRate:
    return MetricRate(
        numerator=numerator,
        denominator=denominator,
        unavailable=unavailable,
        rate_percent=(round(100 * numerator / denominator, 1) if denominator else None),
    )


def _expected_metric(
    results: list[ScenarioResult],
    expected: BenchmarkOutcome,
    successful: BenchmarkOutcome,
) -> MetricRate:
    candidates = [item for item in results if item.identity.expected_outcome == expected]
    available = [
        item for item in candidates if item.actual_outcome != BenchmarkOutcome.OPERATIONAL_FAILURE
    ]
    return _rate(
        sum(item.actual_outcome == successful for item in available),
        len(available),
        len(candidates) - len(available),
    )


def _assertion_metric(
    results: list[ScenarioResult],
    selector: Callable[[ScenarioResult], bool | int | None],
    success: Callable[[bool | int], bool],
) -> MetricRate:
    values = [selector(item) for item in results]
    available = [value for value in values if value is not None]
    return _rate(
        sum(success(value) for value in available),
        len(available),
        len(values) - len(available),
    )


def calculate_metrics(results: list[ScenarioResult]) -> BenchmarkMetrics:
    concurrency = [item for item in results if item.identity.concurrency]
    return BenchmarkMetrics(
        safe_recovery_success=_expected_metric(
            results, BenchmarkOutcome.RECOVERED, BenchmarkOutcome.RECOVERED
        ),
        verified_no_op_correctness=_expected_metric(
            results, BenchmarkOutcome.VERIFIED_NO_OP, BenchmarkOutcome.VERIFIED_NO_OP
        ),
        unsafe_action_rejection=_expected_metric(
            results, BenchmarkOutcome.SAFELY_REJECTED, BenchmarkOutcome.SAFELY_REJECTED
        ),
        duplicate_business_effects=_assertion_metric(
            results,
            lambda item: item.assertions.duplicate_business_effects,
            lambda value: value > 0,
        ),
        invariant_violations=_assertion_metric(
            results,
            lambda item: item.assertions.invariants_preserved,
            lambda value: value is False,
        ),
        audit_consistency=_assertion_metric(
            results,
            lambda item: item.assertions.audit_consistent,
            lambda value: value is True,
        ),
        concurrency_safety=_assertion_metric(
            concurrency,
            lambda item: item.assertions.concurrency_safe,
            lambda value: value is True,
        ),
        operational_completion=_rate(
            sum(item.actual_outcome == item.identity.expected_outcome for item in results),
            len(results),
            0,
        ),
    )


def calculate_latency(
    results: list[ScenarioResult],
) -> tuple[StageLatencySummary, ...]:
    summaries = []
    for stage in BenchmarkStage:
        samples = sorted(
            timing.duration_ms
            for result in results
            for timing in result.timings
            if timing.stage == stage
        )
        if not samples:
            summaries.append(
                StageLatencySummary(
                    stage=stage,
                    count=0,
                    unavailable=len(results),
                )
            )
            continue
        p95_index = max(0, math.ceil(0.95 * len(samples)) - 1)
        summaries.append(
            StageLatencySummary(
                stage=stage,
                count=len(samples),
                unavailable=len(results) - len(samples),
                min_ms=round(samples[0], 3),
                median_ms=round(statistics.median(samples), 3),
                p95_ms=round(samples[p95_index], 3),
                max_ms=round(samples[-1], 3),
            )
        )
    return tuple(summaries)


def _canonical_result(result: ScenarioResult) -> ScenarioResult:
    return result.model_copy(
        update={
            "timings": tuple(sorted(result.timings, key=lambda item: item.stage.value)),
            "limitations": tuple(sorted(set(result.limitations))),
        }
    )


def build_benchmark_report(
    *,
    run: BenchmarkRunMetadata,
    configuration: BenchmarkConfiguration,
    environment: BenchmarkEnvironment,
    results: list[ScenarioResult],
    limitations: list[str] | None = None,
) -> BenchmarkReport:
    ordered = tuple(
        sorted(
            (_canonical_result(item) for item in results),
            key=lambda item: (
                list(EvidenceClass).index(item.identity.evidence_class),
                item.identity.scenario_id,
                item.identity.trial,
            ),
        )
    )
    cohorts = []
    for evidence_class in EvidenceClass:
        cohort_results = [
            item for item in ordered if item.identity.evidence_class == evidence_class
        ]
        cohort_limitations = sorted(
            {limitation for item in cohort_results for limitation in item.limitations}
        )
        cohorts.append(
            EvidenceClassSummary(
                evidence_class=evidence_class,
                trials=len(cohort_results),
                metrics=calculate_metrics(cohort_results),
                latency=calculate_latency(cohort_results),
                limitations=tuple(cohort_limitations),
            )
        )
    return BenchmarkReport(
        run=run,
        configuration=configuration,
        environment=environment,
        results=ordered,
        cohorts=tuple(cohorts),
        limitations=tuple(sorted(set(limitations or []))),
    )


def benchmark_report_json(report: BenchmarkReport) -> str:
    return (
        json.dumps(
            report.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
    )
