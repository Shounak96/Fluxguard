from __future__ import annotations

import re
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from fluxguard.packages.common.models import Incident, IncidentEvidence, InvestigationRun
from fluxguard.packages.contracts.investigations import Diagnosis

PRESENT = "<present>"
EvaluationSource = Literal["historical_live", "historical_fixture"]
OperationalOutcome = Literal[
    "validated_diagnosis",
    "provider_transport_failure",
    "application_or_validation_failure",
]


class StrictEvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EvaluationCase(StrictEvaluationModel):
    case_id: str
    label: str
    incident_type: str
    expected_error_codes: list[str]
    observed_fault_terms: list[str]
    root_cause_terms: list[str]
    acceptable_outcomes: list[Literal["COMPLETED", "INSUFFICIENT_EVIDENCE"]]
    requires_causal_uncertainty: bool
    required_evidence_types: list[str]
    required_evidence_payload: dict[str, list[str | int | float | bool]]
    ground_truth: str


class EvidenceRecord(StrictEvaluationModel):
    evidence_id: UUID
    incident_id: UUID
    evidence_type: str
    payload: dict[str, Any]


class DimensionScore(StrictEvaluationModel):
    score: Literal[0, 1] | None
    reason: str


class EvaluationScores(StrictEvaluationModel):
    observed_fault_recognition: DimensionScore
    underlying_root_cause_correctness: DimensionScore
    evidence_citation_validity: DimensionScore
    appropriate_uncertainty: DimensionScore
    schema_compliance: DimensionScore


class RecordedUsage(StrictEvaluationModel):
    model_id: str
    llm_calls: int
    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None
    latency_seconds: float | None
    estimated_cost_usd: Decimal | None


class InvestigationEvaluation(StrictEvaluationModel):
    case_id: str
    source: EvaluationSource
    investigation_id: UUID
    incident_id: UUID
    persisted_status: str
    diagnosis_outcome: str | None
    operational_outcome: OperationalOutcome
    error_category: str | None
    provider_http_status: int | None
    cited_evidence_ids: list[UUID]
    scores: EvaluationScores
    overall_score: float | None
    usage: RecordedUsage
    notes: list[str] = Field(default_factory=list)


class CaseCoverage(StrictEvaluationModel):
    case_id: str
    persisted_incidents: int
    historical_investigations: int
    evaluated_diagnoses: int


class MetricRate(StrictEvaluationModel):
    numerator: int
    denominator: int
    unavailable: int
    rate_percent: float | None


class DiagnosticMetrics(StrictEvaluationModel):
    observed_fault_recognition: MetricRate
    underlying_root_cause_correctness: MetricRate
    evidence_citation_validity: MetricRate
    appropriate_uncertainty: MetricRate
    schema_compliance: MetricRate


class CohortSummary(StrictEvaluationModel):
    attempts: int
    validated_diagnoses: int
    provider_transport_failures: int
    application_or_validation_failures: int
    operational_completion: MetricRate
    diagnostic_quality: DiagnosticMetrics


class ProviderHttpErrorCount(StrictEvaluationModel):
    http_status: int
    error_category: str
    count: int


class EvaluationSummary(StrictEvaluationModel):
    historical_results: int
    live_provider: CohortSummary
    deterministic_fixtures: CohortSummary
    live_provider_http_errors: list[ProviderHttpErrorCount]
    case_coverage: list[CaseCoverage]


class HistoricalEvaluationReport(StrictEvaluationModel):
    benchmark_execution: Literal["historical_only"] = "historical_only"
    dataset_label: Literal["exploratory_not_statistically_representative"] = (
        "exploratory_not_statistically_representative"
    )
    cases: list[EvaluationCase]
    results: list[InvestigationEvaluation]
    summary: EvaluationSummary


EVALUATION_CASES = (
    EvaluationCase(
        case_id="unsupported_schema_version",
        label="Unsupported schema version",
        incident_type="UNSUPPORTED_SCHEMA_VERSION",
        expected_error_codes=["UNSUPPORTED_SCHEMA_VERSION"],
        observed_fault_terms=[
            "unsupported schema version",
            "schema version 99",
            "unsupported_schema_version",
        ],
        root_cause_terms=[
            "event uses schema version 99",
            "event with schema version 99",
            "published schema version 99",
        ],
        acceptable_outcomes=["COMPLETED", "INSUFFICIENT_EVIDENCE"],
        requires_causal_uncertainty=True,
        required_evidence_types=["QUARANTINED_EVENT"],
        required_evidence_payload={
            "error_code": ["UNSUPPORTED_SCHEMA_VERSION"],
            "schema_version": [99],
        },
        ground_truth=(
            "A v1 consumer quarantined an orders.created.v1 event carrying schema_version 99; "
            "the persisted evidence does not establish why the producer emitted that version."
        ),
    ),
    EvaluationCase(
        case_id="malformed_json_event",
        label="Malformed JSON event",
        incident_type="MALFORMED_EVENT",
        expected_error_codes=["MALFORMED_JSON"],
        observed_fault_terms=["malformed json", "invalid json", "malformed event"],
        root_cause_terms=["producer emitted malformed json", "truncated json payload"],
        acceptable_outcomes=["COMPLETED", "INSUFFICIENT_EVIDENCE"],
        requires_causal_uncertainty=True,
        required_evidence_types=["QUARANTINED_EVENT"],
        required_evidence_payload={"raw_sha256": [PRESENT]},
        ground_truth=(
            "The consumer received non-JSON bytes and quarantined them; the raw-content hash "
            "proves the malformed record but does not identify the producing client."
        ),
    ),
    EvaluationCase(
        case_id="business_invariant_violation",
        label="Business invariant violation",
        incident_type="BUSINESS_INVARIANT_VIOLATION",
        expected_error_codes=[
            "AGGREGATE_ID_MISMATCH",
            "ORDER_TOTAL_MISMATCH",
            "LINE_TOTAL_MISMATCH",
            "CONFLICTING_AGGREGATE_EVENT",
        ],
        observed_fault_terms=[
            "business invariant",
            "order total mismatch",
            "total does not equal",
            "aggregate id mismatch",
        ],
        root_cause_terms=[
            "payload total does not equal its line totals",
            "order total mismatch",
            "aggregate id mismatch",
        ],
        acceptable_outcomes=["COMPLETED", "INSUFFICIENT_EVIDENCE"],
        requires_causal_uncertainty=True,
        required_evidence_types=["QUARANTINED_EVENT"],
        required_evidence_payload={"error_code": ["ORDER_TOTAL_MISMATCH"]},
        ground_truth=(
            "The injected order total conflicts with verified line totals; evidence proves the "
            "invariant failure but not the upstream code defect that created it."
        ),
    ),
    EvaluationCase(
        case_id="poison_message",
        label="Poison message",
        incident_type="POISON_MESSAGE",
        expected_error_codes=["UNPROCESSABLE_POISON_MESSAGE"],
        observed_fault_terms=["poison message", "unprocessable poison", "controlled poison"],
        root_cause_terms=["controlled poison message", "explicit poison scenario"],
        acceptable_outcomes=["COMPLETED"],
        requires_causal_uncertainty=False,
        required_evidence_types=["QUARANTINED_EVENT"],
        required_evidence_payload={
            "error_code": ["UNPROCESSABLE_POISON_MESSAGE"],
            "scenario_id": [PRESENT],
        },
        ground_truth=(
            "An explicit development-only poison_message scenario produced an unprocessable "
            "record that was quarantined."
        ),
    ),
    EvaluationCase(
        case_id="transient_consumer_failure",
        label="Transient consumer failure and retry exhaustion",
        incident_type="REPEATED_TRANSIENT_FAILURE",
        expected_error_codes=["CONSUMER_RETRY_THRESHOLD_EXCEEDED"],
        observed_fault_terms=[
            "transient processing failure",
            "consumer retry threshold",
            "repeated transient failure",
            "retry exhaustion",
        ],
        root_cause_terms=[
            "controlled transient processing failure",
            "transient failure exhausted the retry threshold",
        ],
        acceptable_outcomes=["COMPLETED"],
        requires_causal_uncertainty=False,
        required_evidence_types=["PROCESSING_RETRY"],
        required_evidence_payload={"attempt": [3], "scenario_id": [PRESENT]},
        ground_truth=(
            "The controlled transient_processing_failure scenario reached attempt three, "
            "created an incident, retained the offset, and was expected to recover."
        ),
    ),
)
CASES_BY_INCIDENT_TYPE = {case.incident_type: case for case in EVALUATION_CASES}


def _contains_any(text: str, terms: list[str]) -> bool:
    lowered = text.casefold()
    return any(term.casefold() in lowered for term in terms)


def _diagnosis_text(diagnosis: Diagnosis) -> str:
    hypotheses = [
        item
        for item in [diagnosis.likely_root_cause, *diagnosis.alternative_hypotheses]
        if item is not None
    ]
    parts = [diagnosis.affected_pipeline_component, diagnosis.confidence.rationale]
    for hypothesis in hypotheses:
        parts.extend([hypothesis.claim, hypothesis.rationale])
    parts.extend(diagnosis.proposed_next_investigation_steps)
    parts.extend(diagnosis.uncertainty_missing_information)
    return " ".join(parts)


def _cited_ids(diagnosis: Diagnosis) -> list[UUID]:
    hypotheses = [
        item
        for item in [diagnosis.likely_root_cause, *diagnosis.alternative_hypotheses]
        if item is not None
    ]
    return sorted(
        {
            evidence_id
            for hypothesis in hypotheses
            for evidence_id in (
                *hypothesis.supporting_evidence_ids,
                *hypothesis.contradicting_evidence_ids,
            )
        },
        key=str,
    )


def _evidence_meets_case(record: EvidenceRecord, case: EvaluationCase) -> bool:
    if record.evidence_type not in case.required_evidence_types:
        return False
    for field, expected in case.required_evidence_payload.items():
        actual = record.payload.get(field)
        if PRESENT in expected:
            if actual is None or actual == "":
                return False
        elif actual not in expected:
            return False
    return True


def _score_citations(
    diagnosis: Diagnosis,
    case: EvaluationCase,
    evidence: list[EvidenceRecord],
) -> DimensionScore:
    cited = _cited_ids(diagnosis)
    if not cited:
        if diagnosis.outcome == "INSUFFICIENT_EVIDENCE" and not diagnosis.alternative_hypotheses:
            return DimensionScore(
                score=None,
                reason="no causal claim was made, so no citation was required",
            )
        return DimensionScore(score=0, reason="diagnostic claims did not cite evidence")
    by_id = {item.evidence_id: item for item in evidence}
    unavailable = [item for item in cited if item not in by_id]
    if unavailable:
        return DimensionScore(
            score=0,
            reason=f"{len(unavailable)} cited evidence ID(s) do not belong to the incident",
        )
    if not any(_evidence_meets_case(by_id[item], case) for item in cited):
        return DimensionScore(
            score=0,
            reason="citations exist but do not meet the case evidence requirements",
        )
    return DimensionScore(
        score=1,
        reason="all citations exist, belong to the incident, and include required evidence",
    )


def _score_root_cause(diagnosis: Diagnosis, case: EvaluationCase) -> DimensionScore:
    if diagnosis.outcome == "INSUFFICIENT_EVIDENCE":
        return DimensionScore(
            score=None,
            reason="no underlying root cause was asserted",
        )
    if diagnosis.likely_root_cause is None:
        return DimensionScore(score=0, reason="completed diagnosis omitted a root cause")
    root_text = f"{diagnosis.likely_root_cause.claim} {diagnosis.likely_root_cause.rationale}"
    if _contains_any(root_text, case.root_cause_terms):
        return DimensionScore(score=1, reason="root cause matches deterministic case criteria")
    return DimensionScore(score=0, reason="root cause does not match deterministic case criteria")


def _score_uncertainty(
    diagnosis: Diagnosis,
    case: EvaluationCase,
    root_score: DimensionScore,
) -> DimensionScore:
    if diagnosis.outcome not in case.acceptable_outcomes:
        return DimensionScore(score=0, reason="diagnosis outcome is not acceptable for this case")
    if diagnosis.outcome == "INSUFFICIENT_EVIDENCE":
        valid = diagnosis.likely_root_cause is None and bool(
            diagnosis.uncertainty_missing_information
        )
        return DimensionScore(
            score=1 if valid else 0,
            reason=(
                "uncertainty is explicit and no unsupported root cause is asserted"
                if valid
                else "insufficient-evidence outcome lacks explicit missing information"
            ),
        )
    if root_score.score != 1:
        return DimensionScore(score=0, reason="confident outcome is not supported by root cause")
    if case.requires_causal_uncertainty:
        calibrated = diagnosis.confidence.level != "HIGH" and bool(
            diagnosis.uncertainty_missing_information
        )
        return DimensionScore(
            score=1 if calibrated else 0,
            reason=(
                "causal limitations are explicit"
                if calibrated
                else "diagnosis is more confident than the available causal evidence"
            ),
        )
    return DimensionScore(score=1, reason="case evidence supports a completed outcome")


def _overall_score(scores: EvaluationScores) -> float | None:
    values = [
        item.score
        for item in (
            scores.observed_fault_recognition,
            scores.underlying_root_cause_correctness,
            scores.evidence_citation_validity,
            scores.appropriate_uncertainty,
            scores.schema_compliance,
        )
        if item.score is not None
    ]
    return round(100 * sum(values) / len(values), 1) if values else None


def _provider_http_status(message: str | None) -> int | None:
    if not message:
        return None
    match = re.search(r"\bHTTP\s+(\d{3})\b", message)
    return int(match.group(1)) if match else None


def _failure_outcome(
    error_category: str | None,
    provider_http_status: int | None,
) -> OperationalOutcome:
    if provider_http_status in {400, 422}:
        return "application_or_validation_failure"
    if provider_http_status is not None:
        return "provider_transport_failure"
    if error_category in {"LLM_ERROR", "MODEL_TIMEOUT", "MODEL_RATE_LIMIT"}:
        return "provider_transport_failure"
    return "application_or_validation_failure"


def evaluate_payload(
    *,
    case: EvaluationCase,
    source: EvaluationSource,
    investigation_id: UUID,
    incident_id: UUID,
    persisted_status: str,
    diagnosis_payload: dict[str, Any] | None,
    evidence: list[EvidenceRecord],
    usage: RecordedUsage,
    error_category: str | None = None,
    error_message: str | None = None,
) -> InvestigationEvaluation:
    notes: list[str] = []
    http_status = _provider_http_status(error_message)
    if diagnosis_payload is None:
        notes.append("no persisted diagnosis")
        unavailable = DimensionScore(score=None, reason="diagnosis is unavailable")
        scores = EvaluationScores(
            observed_fault_recognition=unavailable,
            underlying_root_cause_correctness=unavailable,
            evidence_citation_validity=unavailable,
            appropriate_uncertainty=unavailable,
            schema_compliance=unavailable,
        )
        return InvestigationEvaluation(
            case_id=case.case_id,
            source=source,
            investigation_id=investigation_id,
            incident_id=incident_id,
            persisted_status=persisted_status,
            diagnosis_outcome=None,
            operational_outcome=_failure_outcome(error_category, http_status),
            error_category=error_category,
            provider_http_status=http_status,
            cited_evidence_ids=[],
            scores=scores,
            overall_score=_overall_score(scores),
            usage=usage,
            notes=notes,
        )
    try:
        diagnosis = Diagnosis.model_validate(diagnosis_payload)
    except ValidationError as error:
        notes.append(f"diagnosis schema validation failed with {error.error_count()} error(s)")
        scores = EvaluationScores(
            observed_fault_recognition=DimensionScore(
                score=None, reason="diagnosis is unavailable"
            ),
            underlying_root_cause_correctness=DimensionScore(
                score=None, reason="diagnosis is unavailable"
            ),
            evidence_citation_validity=DimensionScore(
                score=None, reason="diagnosis is unavailable"
            ),
            appropriate_uncertainty=DimensionScore(score=None, reason="diagnosis is unavailable"),
            schema_compliance=DimensionScore(
                score=0, reason="payload failed strict Diagnosis validation"
            ),
        )
        return InvestigationEvaluation(
            case_id=case.case_id,
            source=source,
            investigation_id=investigation_id,
            incident_id=incident_id,
            persisted_status=persisted_status,
            diagnosis_outcome=None,
            operational_outcome="application_or_validation_failure",
            error_category=error_category,
            provider_http_status=http_status,
            cited_evidence_ids=[],
            scores=scores,
            overall_score=_overall_score(scores),
            usage=usage,
            notes=notes,
        )

    text = _diagnosis_text(diagnosis)
    observed = _contains_any(text, case.observed_fault_terms)
    root_score = _score_root_cause(diagnosis, case)
    scores = EvaluationScores(
        observed_fault_recognition=DimensionScore(
            score=1 if observed else 0,
            reason=(
                "diagnosis recognizes the expected fault"
                if observed
                else "diagnosis does not recognize the expected fault"
            ),
        ),
        underlying_root_cause_correctness=root_score,
        evidence_citation_validity=_score_citations(diagnosis, case, evidence),
        appropriate_uncertainty=_score_uncertainty(diagnosis, case, root_score),
        schema_compliance=DimensionScore(
            score=1, reason="payload passed strict Diagnosis validation"
        ),
    )
    expected_status = (
        "INSUFFICIENT_EVIDENCE" if diagnosis.outcome == "INSUFFICIENT_EVIDENCE" else "COMPLETED"
    )
    consistency_errors = []
    if diagnosis.investigation_id != investigation_id:
        consistency_errors.append("investigation_id")
    if diagnosis.incident_id != incident_id:
        consistency_errors.append("incident_id")
    if diagnosis.investigation_status.value != persisted_status:
        consistency_errors.append("persisted status")
    if diagnosis.investigation_status.value != expected_status:
        consistency_errors.append("outcome status")
    if consistency_errors:
        scores.schema_compliance = DimensionScore(
            score=0,
            reason="diagnosis consistency failed: " + ", ".join(consistency_errors),
        )
    operational_outcome: OperationalOutcome = (
        "validated_diagnosis"
        if scores.schema_compliance.score == 1
        else "application_or_validation_failure"
    )
    return InvestigationEvaluation(
        case_id=case.case_id,
        source=source,
        investigation_id=investigation_id,
        incident_id=incident_id,
        persisted_status=persisted_status,
        diagnosis_outcome=diagnosis.outcome,
        operational_outcome=operational_outcome,
        error_category=error_category,
        provider_http_status=http_status,
        cited_evidence_ids=_cited_ids(diagnosis),
        scores=scores,
        overall_score=_overall_score(scores),
        usage=usage,
        notes=notes,
    )


def _recorded_usage(run: InvestigationRun) -> RecordedUsage:
    latency = None
    if run.started_at is not None and run.completed_at is not None:
        latency = max(0.0, (run.completed_at - run.started_at).total_seconds())
    return RecordedUsage(
        model_id=run.model_id,
        llm_calls=run.llm_call_count,
        prompt_tokens=run.prompt_tokens,
        completion_tokens=run.completion_tokens,
        total_tokens=run.total_tokens,
        latency_seconds=latency,
        estimated_cost_usd=run.estimated_cost_usd,
    )


def _metric_rate(results: list[InvestigationEvaluation], dimension: str) -> MetricRate:
    scores = [
        getattr(item.scores, dimension).score
        for item in results
        if item.operational_outcome == "validated_diagnosis"
        and getattr(item.scores, dimension).score is not None
    ]
    numerator = sum(score == 1 for score in scores)
    denominator = len(scores)
    return MetricRate(
        numerator=numerator,
        denominator=denominator,
        unavailable=len(results) - denominator,
        rate_percent=round(100 * numerator / denominator, 1) if denominator else None,
    )


def _cohort_summary(results: list[InvestigationEvaluation]) -> CohortSummary:
    validated = sum(item.operational_outcome == "validated_diagnosis" for item in results)
    attempts = len(results)
    return CohortSummary(
        attempts=attempts,
        validated_diagnoses=validated,
        provider_transport_failures=sum(
            item.operational_outcome == "provider_transport_failure" for item in results
        ),
        application_or_validation_failures=sum(
            item.operational_outcome == "application_or_validation_failure" for item in results
        ),
        operational_completion=MetricRate(
            numerator=validated,
            denominator=attempts,
            unavailable=0,
            rate_percent=round(100 * validated / attempts, 1) if attempts else None,
        ),
        diagnostic_quality=DiagnosticMetrics(
            observed_fault_recognition=_metric_rate(results, "observed_fault_recognition"),
            underlying_root_cause_correctness=_metric_rate(
                results, "underlying_root_cause_correctness"
            ),
            evidence_citation_validity=_metric_rate(results, "evidence_citation_validity"),
            appropriate_uncertainty=_metric_rate(results, "appropriate_uncertainty"),
            schema_compliance=_metric_rate(results, "schema_compliance"),
        ),
    )


def summarize_evaluations(
    results: list[InvestigationEvaluation],
    case_coverage: list[CaseCoverage] | None = None,
) -> EvaluationSummary:
    live = [item for item in results if item.source == "historical_live"]
    fixtures = [item for item in results if item.source == "historical_fixture"]
    http_counts: dict[tuple[int, str], int] = {}
    for item in live:
        if item.provider_http_status is None:
            continue
        key = (item.provider_http_status, item.error_category or "UNCLASSIFIED")
        http_counts[key] = http_counts.get(key, 0) + 1
    return EvaluationSummary(
        historical_results=len(results),
        live_provider=_cohort_summary(live),
        deterministic_fixtures=_cohort_summary(fixtures),
        live_provider_http_errors=[
            ProviderHttpErrorCount(http_status=status, error_category=category, count=count)
            for (status, category), count in sorted(http_counts.items())
        ],
        case_coverage=case_coverage or [],
    )


def evaluate_historical(session: Session) -> HistoricalEvaluationReport:
    incident_types = list(CASES_BY_INCIDENT_TYPE)
    candidate_incidents = session.scalars(
        select(Incident).where(Incident.incident_type.in_(incident_types))
    ).all()
    incidents = [
        item
        for item in candidate_incidents
        if item.error_code in CASES_BY_INCIDENT_TYPE[item.incident_type].expected_error_codes
    ]
    incidents_by_id = {item.incident_id: item for item in incidents}
    evidence_rows = session.scalars(
        select(IncidentEvidence).where(IncidentEvidence.incident_id.in_(incidents_by_id))
    ).all()
    evidence_by_incident: dict[UUID, list[EvidenceRecord]] = {}
    for item in evidence_rows:
        evidence_by_incident.setdefault(item.incident_id, []).append(
            EvidenceRecord(
                evidence_id=item.evidence_id,
                incident_id=item.incident_id,
                evidence_type=item.evidence_type,
                payload=item.payload,
            )
        )
    runs = session.scalars(
        select(InvestigationRun)
        .where(InvestigationRun.incident_id.in_(incidents_by_id))
        .order_by(InvestigationRun.created_at, InvestigationRun.investigation_id)
    ).all()
    results: list[InvestigationEvaluation] = []
    for run in runs:
        incident = incidents_by_id[run.incident_id]
        case = CASES_BY_INCIDENT_TYPE[incident.incident_type]
        source: EvaluationSource = (
            "historical_fixture"
            if run.model_id.startswith(("fake/", "integration/fake"))
            else "historical_live"
        )
        result = evaluate_payload(
            case=case,
            source=source,
            investigation_id=run.investigation_id,
            incident_id=run.incident_id,
            persisted_status=run.status,
            diagnosis_payload=run.diagnosis,
            evidence=evidence_by_incident.get(run.incident_id, []),
            usage=_recorded_usage(run),
            error_category=run.error_category,
            error_message=run.error_message,
        )
        if incident.error_code not in case.expected_error_codes:
            result.notes.append(
                f"incident error_code {incident.error_code} is outside case expectations"
            )
        results.append(result)

    coverage = []
    for case in EVALUATION_CASES:
        case_incident_ids = {
            item.incident_id for item in incidents if item.incident_type == case.incident_type
        }
        case_results = [item for item in results if item.case_id == case.case_id]
        coverage.append(
            CaseCoverage(
                case_id=case.case_id,
                persisted_incidents=len(case_incident_ids),
                historical_investigations=len(case_results),
                evaluated_diagnoses=sum(
                    item.scores.schema_compliance.score == 1 for item in case_results
                ),
            )
        )
    summary = summarize_evaluations(results, coverage)
    return HistoricalEvaluationReport(
        cases=list(EVALUATION_CASES),
        results=results,
        summary=summary,
    )


def terminal_summary(report: HistoricalEvaluationReport) -> str:
    summary = report.summary
    live = summary.live_provider
    fixtures = summary.deterministic_fixtures
    operational_rate = (
        f"{live.operational_completion.rate_percent}%"
        if live.operational_completion.rate_percent is not None
        else "unavailable"
    )

    def metric(name: str, item: MetricRate) -> str:
        rate = f"{item.rate_percent}%" if item.rate_percent is not None else "unavailable"
        return (
            f"  {name}: {item.numerator}/{item.denominator} ({rate}); "
            f"unavailable={item.unavailable}"
        )

    lines = [
        "FluxGuard investigation evaluation (exploratory historical records only)",
        "Dataset is not statistically representative.",
        f"Historical results: {summary.historical_results}",
        (
            f"Live operational completion: {live.operational_completion.numerator}/"
            f"{live.operational_completion.denominator} "
            f"({operational_rate}); "
            f"provider/transport failures={live.provider_transport_failures}; "
            f"application/validation failures={live.application_or_validation_failures}"
        ),
        "Live diagnostic quality (validated diagnoses only):",
        metric("fault recognition", live.diagnostic_quality.observed_fault_recognition),
        metric("root cause", live.diagnostic_quality.underlying_root_cause_correctness),
        metric("evidence citations", live.diagnostic_quality.evidence_citation_validity),
        metric("appropriate uncertainty", live.diagnostic_quality.appropriate_uncertainty),
        metric("schema compliance", live.diagnostic_quality.schema_compliance),
        (
            f"Deterministic fixtures (not live performance): attempts={fixtures.attempts}; "
            f"validated={fixtures.validated_diagnoses}"
        ),
    ]
    if summary.live_provider_http_errors:
        lines.append(
            "Live provider HTTP errors: "
            + ", ".join(
                f"HTTP {item.http_status}/{item.error_category}={item.count}"
                for item in summary.live_provider_http_errors
            )
        )
    for item in summary.case_coverage:
        lines.append(
            f"- {item.case_id}: incidents={item.persisted_incidents}, "
            f"investigations={item.historical_investigations}, "
            f"valid_diagnoses={item.evaluated_diagnoses}"
        )
    return "\n".join(lines)
