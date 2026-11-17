from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pydantic import ValidationError

from fluxguard.apps import observe_benchmark
from fluxguard.packages.common.benchmark_metrics import (
    benchmark_report_json,
    build_benchmark_report,
)
from fluxguard.packages.common.benchmark_observability import (
    build_operational_summary,
    classify_scenario,
    load_recovery_audit_trace,
    operational_summary_json,
)
from fluxguard.packages.contracts.benchmarks import (
    AssertionResults,
    AuditLifecycleReference,
    AuditReconciliationStatus,
    BenchmarkConfiguration,
    BenchmarkEnvironment,
    BenchmarkMode,
    BenchmarkOutcome,
    BenchmarkResourceNames,
    BenchmarkRunMetadata,
    BenchmarkStage,
    EvidenceClass,
    ObservableLifecycleState,
    OperationalDisposition,
    RecoveryAuditTrace,
    SanitizedError,
    ScenarioIdentity,
    ScenarioResult,
    StageTiming,
)

RUN_ID = UUID("12345678-1234-4234-8234-123456789abc")
INCIDENT_ID = UUID("10000000-0000-4000-8000-000000000001")
PLAN_ID = UUID("20000000-0000-4000-8000-000000000002")
VERIFICATION_ID = UUID("30000000-0000-4000-8000-000000000003")
APPROVAL_ID = UUID("40000000-0000-4000-8000-000000000004")
EXECUTION_ID = UUID("50000000-0000-4000-8000-000000000005")


def assertions(*, mutation: bool = True, audit: bool = True) -> AssertionResults:
    return AssertionResults(
        invariants_preserved=True,
        declared_write_set_only=True,
        processed_event_cardinality_valid=True,
        projection_cardinality_valid=True,
        outbox_effect_valid=True,
        duplicate_business_effects=0,
        unexpected_kafka_publications=0,
        audit_consistent=audit,
        business_mutation_applied=mutation,
    )


def complete_trace(
    *, status: AuditReconciliationStatus = AuditReconciliationStatus.CONSISTENT
) -> RecoveryAuditTrace:
    states = (
        ObservableLifecycleState.PROPOSED,
        ObservableLifecycleState.VERIFIED,
        ObservableLifecycleState.AWAITING_APPROVAL,
        ObservableLifecycleState.APPROVED,
        ObservableLifecycleState.EXECUTING,
        ObservableLifecycleState.EXECUTED,
    )
    return RecoveryAuditTrace(
        incident_id=INCIDENT_ID,
        plan_id=PLAN_ID,
        verification_id=VERIFICATION_ID,
        approval_id=APPROVAL_ID,
        execution_id=EXECUTION_ID,
        lifecycle=tuple(
            AuditLifecycleReference(
                event_id=UUID(int=index),
                sequence_number=index,
                state=state,
                reason_code=f"LIFECYCLE_{state.value}",
            )
            for index, state in enumerate(states, start=1)
        ),
        execution_reason_codes=("EXECUTION_STARTED", "EXECUTION_COMMITTED"),
        reconciliation_status=status,
        reconciliation_issues=("TERMINAL_AUDIT_MISMATCH",)
        if status == AuditReconciliationStatus.INCONSISTENT
        else (),
    )


def scenario(
    *,
    scenario_id: str = "replay",
    expected: BenchmarkOutcome = BenchmarkOutcome.RECOVERED,
    actual: BenchmarkOutcome = BenchmarkOutcome.RECOVERED,
    error_category: str | None = None,
    audit_trace: RecoveryAuditTrace | None = None,
    timings: tuple[StageTiming, ...] = (),
) -> ScenarioResult:
    error = (
        SanitizedError(
            category=error_category,
            stage=BenchmarkStage.EXECUTION,
            retryable=False,
            message="operation ended safely",
        )
        if error_category
        else None
    )
    return ScenarioResult(
        identity=ScenarioIdentity(
            scenario_id=scenario_id,
            trial=1,
            evidence_class=EvidenceClass.REAL_INTEGRATION,
            expected_outcome=expected,
        ),
        actual_outcome=actual,
        assertions=(
            assertions(mutation=actual == BenchmarkOutcome.RECOVERED)
            if actual != BenchmarkOutcome.OPERATIONAL_FAILURE
            else assertions(mutation=True)
        ),
        error=error,
        audit_trace=audit_trace,
        timings=timings,
    )


def report(results: list[ScenarioResult]):
    return build_benchmark_report(
        run=BenchmarkRunMetadata(
            run_id=RUN_ID,
            started_at=datetime(2026, 1, 1, tzinfo=UTC),
            completed_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            duration_ms=1000,
        ),
        configuration=BenchmarkConfiguration(
            benchmark_name="observability-test",
            mode=BenchmarkMode.RECOVERY_SAFETY,
            seed=1,
            serial_trials=1,
            concurrency_trials=3,
            resources=BenchmarkResourceNames(
                database_name=f"fluxguard_benchmark_{RUN_ID.hex}",
                kafka_topic=f"fluxguard-benchmark-{RUN_ID.hex}-orders",
                kafka_consumer_group=f"fluxguard-benchmark-{RUN_ID.hex}-consumer",
            ),
        ),
        environment=BenchmarkEnvironment(python_version="3.12"),
        results=results,
    )


def test_complete_lifecycle_trace_and_correlations_are_preserved() -> None:
    summary = build_operational_summary(report([scenario(audit_trace=complete_trace())]))
    trace = summary.traces[0]

    assert trace.run_id == RUN_ID
    assert trace.incident_id == INCIDENT_ID
    assert trace.plan_id == PLAN_ID
    assert trace.verification_id == VERIFICATION_ID
    assert trace.approval_id == APPROVAL_ID
    assert trace.execution_id == EXECUTION_ID
    assert [event.state for event in trace.lifecycle] == [
        ObservableLifecycleState.PROPOSED,
        ObservableLifecycleState.VERIFIED,
        ObservableLifecycleState.AWAITING_APPROVAL,
        ObservableLifecycleState.APPROVED,
        ObservableLifecycleState.EXECUTING,
        ObservableLifecycleState.EXECUTED,
    ]
    assert trace.reconciliation_status == AuditReconciliationStatus.CONSISTENT


def test_persisted_audit_rows_build_a_complete_reconciled_trace() -> None:
    lifecycle_rows = [
        SimpleNamespace(
            event_id=UUID(int=index),
            sequence_number=index,
            to_state=state,
            reason_code=f"LIFECYCLE_{state}",
        )
        for index, state in enumerate(
            ("PROPOSED", "VERIFIED", "AWAITING_APPROVAL", "APPROVED", "EXECUTING", "SUCCEEDED"),
            start=1,
        )
    ]
    execution_rows = [
        SimpleNamespace(sequence_number=1, reason_code="EXECUTION_STARTED"),
        SimpleNamespace(sequence_number=2, reason_code="EXECUTION_COMMITTED"),
    ]
    verification = SimpleNamespace(
        verification_id=VERIFICATION_ID,
        reason_codes=[],
    )
    approval = SimpleNamespace(
        approval_id=APPROVAL_ID,
        verification_id=VERIFICATION_ID,
    )
    session = MagicMock()
    session.get.side_effect = [
        SimpleNamespace(incident_id=INCIDENT_ID),
        SimpleNamespace(plan_id=PLAN_ID),
    ]
    session.scalar.side_effect = [verification, approval]
    session.scalars.side_effect = [
        SimpleNamespace(all=lambda: lifecycle_rows),
        SimpleNamespace(all=lambda: execution_rows),
    ]

    trace = load_recovery_audit_trace(
        session,
        incident_id=INCIDENT_ID,
        plan_id=PLAN_ID,
        execution_id=EXECUTION_ID,
    )

    assert trace.verification_id == VERIFICATION_ID
    assert trace.approval_id == APPROVAL_ID
    assert trace.execution_id == EXECUTION_ID
    assert trace.lifecycle[-1].state == ObservableLifecycleState.EXECUTED
    assert trace.execution_reason_codes == ("EXECUTION_STARTED", "EXECUTION_COMMITTED")
    assert trace.reconciliation_status == AuditReconciliationStatus.CONSISTENT


def test_missing_audit_references_remain_unavailable_and_are_not_fabricated() -> None:
    summary = build_operational_summary(report([scenario()]))
    trace = summary.traces[0]
    missing = summary.cohorts[0].missing_telemetry

    assert trace.incident_id is None
    assert trace.plan_id is None
    assert trace.reconciliation_status == AuditReconciliationStatus.UNAVAILABLE
    assert trace.reconciliation_issues == ("AUDIT_TRACE_UNAVAILABLE",)
    assert missing.audit_trace == 1
    assert missing.incident_id == 1
    assert missing.execution_id == 1


def test_expected_conflict_is_separate_from_unexpected_operational_failure() -> None:
    expected = scenario(
        scenario_id="consumer-race",
        expected=BenchmarkOutcome.OPERATIONAL_FAILURE,
        actual=BenchmarkOutcome.OPERATIONAL_FAILURE,
        error_category="CONCURRENT_CONSUMER_WON",
    )
    unexpected = scenario(
        scenario_id="provider-down",
        expected=BenchmarkOutcome.RECOVERED,
        actual=BenchmarkOutcome.OPERATIONAL_FAILURE,
        error_category="INFRASTRUCTURE_UNAVAILABLE",
    )

    assert classify_scenario(expected) == OperationalDisposition.EXPECTED_OPERATIONAL_CONFLICT
    assert classify_scenario(unexpected) == OperationalDisposition.UNEXPECTED_OPERATIONAL_FAILURE
    outcomes = build_operational_summary(report([expected, unexpected])).cohorts[0].outcomes
    assert outcomes.expected_operational_conflict == 1
    assert outcomes.unexpected_operational_failure == 1


def test_latency_samples_and_missing_counts_are_reused_without_fabrication() -> None:
    summary = build_operational_summary(
        report(
            [
                scenario(
                    scenario_id="timed",
                    timings=(
                        StageTiming(stage=BenchmarkStage.EXECUTION, duration_ms=40),
                        StageTiming(stage=BenchmarkStage.END_TO_END, duration_ms=100),
                    ),
                ),
                scenario(scenario_id="untimed"),
            ]
        )
    )
    latency = {item.stage: item for item in summary.cohorts[0].latency}

    assert latency[BenchmarkStage.EXECUTION].count == 1
    assert latency[BenchmarkStage.EXECUTION].unavailable == 1
    assert latency[BenchmarkStage.EXECUTION].median_ms == 40
    assert latency[BenchmarkStage.APPROVAL].count == 0
    assert latency[BenchmarkStage.APPROVAL].median_ms is None


def test_safe_rejection_reasons_are_counted_from_sanitized_audit_data() -> None:
    rejected_trace = RecoveryAuditTrace(
        incident_id=INCIDENT_ID,
        plan_id=PLAN_ID,
        verification_id=VERIFICATION_ID,
        lifecycle=(
            AuditLifecycleReference(
                event_id=UUID(int=1),
                sequence_number=1,
                state=ObservableLifecycleState.PROPOSED,
                reason_code="PLAN_CREATED",
            ),
            AuditLifecycleReference(
                event_id=UUID(int=2),
                sequence_number=2,
                state=ObservableLifecycleState.REJECTED,
                reason_code="VERIFICATION_REJECTED",
            ),
        ),
        safe_reason_codes=("UNSUPPORTED_SCHEMA",),
        verification_reason_codes=("UNSUPPORTED_SCHEMA",),
        reconciliation_status=AuditReconciliationStatus.CONSISTENT,
    )
    rejected = scenario(
        scenario_id="unsupported-schema",
        expected=BenchmarkOutcome.SAFELY_REJECTED,
        actual=BenchmarkOutcome.SAFELY_REJECTED,
        audit_trace=rejected_trace,
    )
    summary = build_operational_summary(report([rejected]))

    assert [item.model_dump() for item in summary.cohorts[0].safe_rejection_reasons] == [
        {"code": "UNSUPPORTED_SCHEMA", "count": 1}
    ]


def test_sensitive_reason_codes_are_rejected() -> None:
    with pytest.raises(ValidationError):
        RecoveryAuditTrace(
            incident_id=INCIDENT_ID,
            verification_reason_codes=("password=secret",),
            reconciliation_status=AuditReconciliationStatus.UNAVAILABLE,
        )


def test_audit_report_inconsistency_is_exposed() -> None:
    summary = build_operational_summary(
        report(
            [
                scenario(
                    expected=BenchmarkOutcome.VERIFIED_NO_OP,
                    actual=BenchmarkOutcome.VERIFIED_NO_OP,
                    audit_trace=complete_trace(),
                )
            ]
        )
    )

    assert summary.traces[0].reconciliation_status == AuditReconciliationStatus.INCONSISTENT
    assert summary.traces[0].reconciliation_issues == ("OUTCOME_AUDIT_MISMATCH",)


def test_summary_json_is_stable_and_contains_no_environment_secrets() -> None:
    summary = build_operational_summary(report([scenario(audit_trace=complete_trace())]))
    first = operational_summary_json(summary)
    second = operational_summary_json(summary)

    assert first == second
    assert first.startswith('{"benchmark_report_schema_version":2')
    assert "postgresql://" not in first
    assert "raw_payload" not in first
    assert "api_key" not in first.lower()


def test_cli_summarizes_existing_report_without_running_benchmark(
    tmp_path, monkeypatch, capsys
) -> None:
    stored_report = report([scenario(audit_trace=complete_trace())])
    report_path = tmp_path / "benchmark.json"
    report_path.write_text(benchmark_report_json(stored_report), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "sys.argv",
        ["fluxguard-observe-benchmark", "benchmark.json", "--format", "text"],
    )

    assert observe_benchmark.main() == 0
    output = capsys.readouterr().out
    assert f"run={RUN_ID}" in output
    assert "recovered=1" in output
