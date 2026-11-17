import json
from collections import Counter
from pathlib import Path
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from fluxguard.packages.common.models import (
    RecoveryApproval,
    RecoveryExecutionEvent,
    RecoveryExecutionRequest,
    RecoveryLifecycleEvent,
    RecoveryPlanEntity,
    RecoveryVerification,
)
from fluxguard.packages.contracts.benchmarks import (
    AuditLifecycleReference,
    AuditReconciliationStatus,
    BenchmarkOperationalSummary,
    BenchmarkOutcome,
    BenchmarkReport,
    CodeCount,
    EvidenceOperationalSummary,
    MissingTelemetryCounts,
    ObservableLifecycleState,
    OperationalDisposition,
    OperationalOutcomeCounts,
    RecoveryAuditTrace,
    ScenarioOperationalTrace,
    ScenarioResult,
)

_LIFECYCLE_LABELS = {
    "PROPOSED": ObservableLifecycleState.PROPOSED,
    "VERIFIED": ObservableLifecycleState.VERIFIED,
    "REJECTED": ObservableLifecycleState.REJECTED,
    "AWAITING_APPROVAL": ObservableLifecycleState.AWAITING_APPROVAL,
    "APPROVED": ObservableLifecycleState.APPROVED,
    "EXECUTING": ObservableLifecycleState.EXECUTING,
    "SUCCEEDED": ObservableLifecycleState.EXECUTED,
    "NO_OP": ObservableLifecycleState.NO_OP,
    "FAILED": ObservableLifecycleState.FAILED,
    "EXPIRED": ObservableLifecycleState.EXPIRED,
}


class BenchmarkObservabilityError(RuntimeError):
    pass


def load_recovery_audit_trace(
    session: Session,
    *,
    incident_id: UUID,
    plan_id: UUID | None,
    execution_id: UUID | None = None,
    additional_reason_codes: tuple[str, ...] = (),
) -> RecoveryAuditTrace:
    if plan_id is None:
        return RecoveryAuditTrace(
            incident_id=incident_id,
            safe_reason_codes=additional_reason_codes,
            reconciliation_status=AuditReconciliationStatus.UNAVAILABLE,
            reconciliation_issues=("PLAN_IDENTIFIER_UNAVAILABLE",),
        )
    plan = session.get(RecoveryPlanEntity, plan_id)
    if plan is None:
        return RecoveryAuditTrace(
            incident_id=incident_id,
            plan_id=plan_id,
            safe_reason_codes=additional_reason_codes,
            reconciliation_status=AuditReconciliationStatus.UNAVAILABLE,
            reconciliation_issues=("PLAN_NOT_PERSISTED",),
        )

    verification = session.scalar(
        select(RecoveryVerification).where(RecoveryVerification.plan_id == plan_id)
    )
    approval = session.scalar(select(RecoveryApproval).where(RecoveryApproval.plan_id == plan_id))
    lifecycle_rows = session.scalars(
        select(RecoveryLifecycleEvent)
        .where(RecoveryLifecycleEvent.plan_id == plan_id)
        .order_by(RecoveryLifecycleEvent.sequence_number)
    ).all()
    request = session.get(RecoveryExecutionRequest, execution_id) if execution_id else None
    execution_rows = (
        session.scalars(
            select(RecoveryExecutionEvent)
            .where(RecoveryExecutionEvent.execution_id == execution_id)
            .order_by(RecoveryExecutionEvent.sequence_number)
        ).all()
        if execution_id
        else []
    )

    issues: list[str] = []
    if plan.incident_id != incident_id:
        issues.append("INCIDENT_PLAN_MISMATCH")
    lifecycle_sequences = [row.sequence_number for row in lifecycle_rows]
    if lifecycle_sequences != list(range(1, len(lifecycle_rows) + 1)):
        issues.append("LIFECYCLE_SEQUENCE_GAP")
    if not lifecycle_rows:
        issues.append("LIFECYCLE_MISSING")
    if approval is not None and (
        verification is None or approval.verification_id != verification.verification_id
    ):
        issues.append("APPROVAL_VERIFICATION_MISMATCH")
    if execution_id is not None and request is None:
        issues.append("EXECUTION_REQUEST_MISSING")
    if request is not None and request.plan_id != plan_id:
        issues.append("EXECUTION_PLAN_MISMATCH")
    execution_sequences = [row.sequence_number for row in execution_rows]
    if execution_rows and execution_sequences != list(range(1, len(execution_rows) + 1)):
        issues.append("EXECUTION_SEQUENCE_GAP")

    lifecycle = tuple(
        AuditLifecycleReference(
            event_id=row.event_id,
            sequence_number=row.sequence_number,
            state=_LIFECYCLE_LABELS[row.to_state],
            reason_code=row.reason_code,
        )
        for row in lifecycle_rows
    )
    return RecoveryAuditTrace(
        incident_id=incident_id,
        plan_id=plan_id,
        verification_id=verification.verification_id if verification else None,
        approval_id=approval.approval_id if approval else None,
        execution_id=execution_id if request else None,
        lifecycle=lifecycle,
        safe_reason_codes=tuple(
            sorted(
                set(
                    (
                        *(verification.reason_codes if verification else ()),
                        *(row.reason_code for row in execution_rows),
                        *additional_reason_codes,
                    )
                )
            )
        ),
        verification_reason_codes=tuple(verification.reason_codes) if verification else (),
        execution_reason_codes=tuple(row.reason_code for row in execution_rows),
        reconciliation_status=(
            AuditReconciliationStatus.INCONSISTENT
            if issues
            else AuditReconciliationStatus.CONSISTENT
        ),
        reconciliation_issues=tuple(sorted(set(issues))),
    )


def _has_safety_assertion_failure(result: ScenarioResult) -> bool:
    assertions = result.assertions
    return any(
        (
            assertions.invariants_preserved is False,
            assertions.declared_write_set_only is False,
            assertions.processed_event_cardinality_valid is False,
            assertions.projection_cardinality_valid is False,
            assertions.outbox_effect_valid is False,
            (assertions.duplicate_business_effects or 0) > 0,
            (assertions.unexpected_kafka_publications or 0) > 0,
            assertions.audit_consistent is False,
            assertions.concurrency_safe is False,
            result.error is not None and result.error.category == "BENCHMARK_ASSERTION_FAILED",
        )
    )


def classify_scenario(result: ScenarioResult) -> OperationalDisposition:
    if _has_safety_assertion_failure(result):
        return OperationalDisposition.SAFETY_ASSERTION_FAILURE
    if result.actual_outcome == BenchmarkOutcome.RECOVERED:
        return OperationalDisposition.RECOVERY_EXECUTION_SUCCESS
    if result.actual_outcome == BenchmarkOutcome.VERIFIED_NO_OP:
        return OperationalDisposition.VERIFIED_NO_OP
    if result.actual_outcome == BenchmarkOutcome.SAFELY_REJECTED:
        return OperationalDisposition.SAFE_REJECTION
    if result.identity.expected_outcome != BenchmarkOutcome.OPERATIONAL_FAILURE:
        return OperationalDisposition.UNEXPECTED_OPERATIONAL_FAILURE
    if result.error is not None and result.error.category.startswith("CONCURRENT_"):
        return OperationalDisposition.EXPECTED_OPERATIONAL_CONFLICT
    return OperationalDisposition.EXPECTED_OPERATIONAL_FAILURE


def _safe_rejection_codes(result: ScenarioResult) -> tuple[str, ...]:
    if result.actual_outcome != BenchmarkOutcome.SAFELY_REJECTED or result.audit_trace is None:
        return ()
    if result.audit_trace.safe_reason_codes:
        return result.audit_trace.safe_reason_codes
    if result.audit_trace.lifecycle:
        terminal = result.audit_trace.lifecycle[-1]
        if terminal.state in {
            ObservableLifecycleState.REJECTED,
            ObservableLifecycleState.EXPIRED,
            ObservableLifecycleState.APPROVED,
        }:
            return (terminal.reason_code,)
    return ()


def _outcome_matches_audit(result: ScenarioResult, audit: RecoveryAuditTrace) -> bool | None:
    if audit.reconciliation_status == AuditReconciliationStatus.UNAVAILABLE:
        return None
    terminal = audit.lifecycle[-1].state if audit.lifecycle else None
    if result.actual_outcome == BenchmarkOutcome.RECOVERED:
        return terminal == ObservableLifecycleState.EXECUTED
    if result.actual_outcome == BenchmarkOutcome.VERIFIED_NO_OP:
        return terminal == ObservableLifecycleState.NO_OP
    if result.actual_outcome == BenchmarkOutcome.SAFELY_REJECTED:
        return terminal in {
            ObservableLifecycleState.REJECTED,
            ObservableLifecycleState.EXPIRED,
        } or (
            terminal == ObservableLifecycleState.APPROVED
            and "EXECUTION_ROLLED_BACK" in audit.execution_reason_codes
        )
    return (
        "EXECUTION_ROLLED_BACK" in audit.execution_reason_codes
        or terminal == ObservableLifecycleState.FAILED
    )


def _trace(report: BenchmarkReport, result: ScenarioResult) -> ScenarioOperationalTrace:
    audit = result.audit_trace
    status = (
        audit.reconciliation_status if audit is not None else AuditReconciliationStatus.UNAVAILABLE
    )
    issues = list(audit.reconciliation_issues if audit else ("AUDIT_TRACE_UNAVAILABLE",))
    if (
        result.assertions.audit_consistent is False
        and "BENCHMARK_AUDIT_ASSERTION_FAILED" not in issues
    ):
        issues.append("BENCHMARK_AUDIT_ASSERTION_FAILED")
        status = AuditReconciliationStatus.INCONSISTENT
    if audit is not None and _outcome_matches_audit(result, audit) is False:
        issues.append("OUTCOME_AUDIT_MISMATCH")
        status = AuditReconciliationStatus.INCONSISTENT
    safe_codes = tuple(sorted(set(audit.safe_reason_codes if audit else ())))
    return ScenarioOperationalTrace(
        run_id=report.run.run_id,
        scenario_id=result.identity.scenario_id,
        trial=result.identity.trial,
        evidence_class=result.identity.evidence_class,
        disposition=classify_scenario(result),
        incident_id=audit.incident_id if audit else None,
        plan_id=audit.plan_id if audit else None,
        verification_id=audit.verification_id if audit else None,
        approval_id=audit.approval_id if audit else None,
        execution_id=audit.execution_id if audit else None,
        lifecycle=audit.lifecycle if audit else (),
        safe_reason_codes=safe_codes,
        reconciliation_status=status,
        reconciliation_issues=tuple(sorted(set(issues))),
    )


def _missing_telemetry(results: list[ScenarioResult]) -> MissingTelemetryCounts:
    traces = [result.audit_trace for result in results]
    return MissingTelemetryCounts(
        audit_trace=sum(trace is None for trace in traces),
        incident_id=sum(trace is None for trace in traces),
        plan_id=sum(trace is None or trace.plan_id is None for trace in traces),
        verification_id=sum(trace is None or trace.verification_id is None for trace in traces),
        approval_id=sum(trace is None or trace.approval_id is None for trace in traces),
        execution_id=sum(trace is None or trace.execution_id is None for trace in traces),
    )


def build_operational_summary(report: BenchmarkReport) -> BenchmarkOperationalSummary:
    traces = tuple(_trace(report, result) for result in report.results)
    cohorts = []
    for cohort in report.cohorts:
        results = [
            result
            for result in report.results
            if result.identity.evidence_class == cohort.evidence_class
        ]
        dispositions = Counter(classify_scenario(result) for result in results)
        reasons = Counter(code for result in results for code in _safe_rejection_codes(result))
        cohorts.append(
            EvidenceOperationalSummary(
                evidence_class=cohort.evidence_class,
                trials=len(results),
                outcomes=OperationalOutcomeCounts(
                    recovery_execution_success=dispositions[
                        OperationalDisposition.RECOVERY_EXECUTION_SUCCESS
                    ],
                    verified_no_op=dispositions[OperationalDisposition.VERIFIED_NO_OP],
                    safe_rejection=dispositions[OperationalDisposition.SAFE_REJECTION],
                    expected_operational_conflict=dispositions[
                        OperationalDisposition.EXPECTED_OPERATIONAL_CONFLICT
                    ],
                    expected_operational_failure=dispositions[
                        OperationalDisposition.EXPECTED_OPERATIONAL_FAILURE
                    ],
                    unexpected_operational_failure=dispositions[
                        OperationalDisposition.UNEXPECTED_OPERATIONAL_FAILURE
                    ],
                    safety_assertion_failure=dispositions[
                        OperationalDisposition.SAFETY_ASSERTION_FAILURE
                    ],
                ),
                safe_rejection_reasons=tuple(
                    CodeCount(code=code, count=count) for code, count in sorted(reasons.items())
                ),
                metrics=cohort.metrics,
                latency=cohort.latency,
                missing_telemetry=_missing_telemetry(results),
            )
        )
    return BenchmarkOperationalSummary(
        run_id=report.run.run_id,
        benchmark_report_schema_version=report.schema_version,
        total_scenarios=len(report.results),
        cohorts=tuple(cohorts),
        traces=traces,
        limitations=report.limitations,
    )


def operational_summary_json(summary: BenchmarkOperationalSummary) -> str:
    return (
        json.dumps(
            summary.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
    )


def load_benchmark_report(path: Path) -> BenchmarkReport:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return BenchmarkReport.model_validate(payload)
    except (OSError, json.JSONDecodeError, ValidationError) as error:
        raise BenchmarkObservabilityError("benchmark report is unavailable or invalid") from error
