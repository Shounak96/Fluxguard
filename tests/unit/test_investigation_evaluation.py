from decimal import Decimal
from uuid import UUID, uuid4

from fluxguard.packages.common.investigation_evaluation import (
    CASES_BY_INCIDENT_TYPE,
    PRESENT,
    EvidenceRecord,
    RecordedUsage,
    evaluate_payload,
    summarize_evaluations,
)
from fluxguard.packages.common.models import InvestigationStatus
from fluxguard.packages.contracts.investigations import (
    ConfidenceAssessment,
    Diagnosis,
    EvidenceHypothesis,
)


def usage() -> RecordedUsage:
    return RecordedUsage(
        model_id="fixture/free",
        llm_calls=2,
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
        latency_seconds=1.25,
        estimated_cost_usd=Decimal("0"),
    )


def case_evidence(incident_type: str, incident_id: UUID, evidence_id: UUID) -> EvidenceRecord:
    case = CASES_BY_INCIDENT_TYPE[incident_type]
    payload = {
        field: "fixture-value" if PRESENT in expected else expected[0]
        for field, expected in case.required_evidence_payload.items()
    }
    return EvidenceRecord(
        evidence_id=evidence_id,
        incident_id=incident_id,
        evidence_type=case.required_evidence_types[0],
        payload=payload,
    )


def completed_diagnosis(
    *,
    investigation_id: UUID,
    incident_id: UUID,
    evidence_id: UUID,
    claim: str,
    confidence: str = "MEDIUM",
) -> Diagnosis:
    return Diagnosis(
        outcome="COMPLETED",
        likely_root_cause=EvidenceHypothesis(
            claim=claim,
            rationale=claim,
            supporting_evidence_ids=[evidence_id],
        ),
        alternative_hypotheses=[],
        confidence=ConfidenceAssessment(
            level=confidence,
            rationale="The deterministic fixture provides direct evidence.",
        ),
        affected_pipeline_component="order_consumer",
        proposed_next_investigation_steps=[],
        uncertainty_missing_information=[],
        investigation_id=investigation_id,
        incident_id=incident_id,
        investigation_status=InvestigationStatus.COMPLETED,
        model_id="fixture/free",
        llm_calls=2,
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
        estimated_cost_usd=Decimal("0"),
        elapsed_seconds=1.25,
    )


def evaluate(
    incident_type: str,
    diagnosis_payload: dict | None,
    *,
    investigation_id: UUID,
    incident_id: UUID,
    evidence: list[EvidenceRecord],
    persisted_status: str,
    source: str = "historical_fixture",
    error_category: str | None = None,
    error_message: str | None = None,
):
    return evaluate_payload(
        case=CASES_BY_INCIDENT_TYPE[incident_type],
        source=source,
        investigation_id=investigation_id,
        incident_id=incident_id,
        persisted_status=persisted_status,
        diagnosis_payload=diagnosis_payload,
        evidence=evidence,
        usage=usage(),
        error_category=error_category,
        error_message=error_message,
    )


def test_correct_grounded_diagnosis_scores_every_dimension() -> None:
    investigation_id, incident_id, evidence_id = uuid4(), uuid4(), uuid4()
    evidence = case_evidence("POISON_MESSAGE", incident_id, evidence_id)
    diagnosis = completed_diagnosis(
        investigation_id=investigation_id,
        incident_id=incident_id,
        evidence_id=evidence_id,
        claim="The explicit poison scenario created a controlled poison message.",
    )

    result = evaluate(
        "POISON_MESSAGE",
        diagnosis.model_dump(mode="json"),
        investigation_id=investigation_id,
        incident_id=incident_id,
        evidence=[evidence],
        persisted_status="COMPLETED",
    )

    assert result.overall_score == 100.0
    assert result.scores.observed_fault_recognition.score == 1
    assert result.scores.underlying_root_cause_correctness.score == 1
    assert result.scores.evidence_citation_validity.score == 1


def test_incorrect_confident_diagnosis_is_not_rewarded() -> None:
    investigation_id, incident_id, evidence_id = uuid4(), uuid4(), uuid4()
    evidence = case_evidence("POISON_MESSAGE", incident_id, evidence_id)
    diagnosis = completed_diagnosis(
        investigation_id=investigation_id,
        incident_id=incident_id,
        evidence_id=evidence_id,
        claim="A database connection pool outage caused processing to stop.",
        confidence="HIGH",
    )

    result = evaluate(
        "POISON_MESSAGE",
        diagnosis.model_dump(mode="json"),
        investigation_id=investigation_id,
        incident_id=incident_id,
        evidence=[evidence],
        persisted_status="COMPLETED",
    )

    assert result.scores.observed_fault_recognition.score == 0
    assert result.scores.underlying_root_cause_correctness.score == 0
    assert result.scores.appropriate_uncertainty.score == 0
    assert result.overall_score < 50


def test_hallucinated_citation_fails_independent_evidence_score() -> None:
    investigation_id, incident_id = uuid4(), uuid4()
    real_evidence_id, invented_evidence_id = uuid4(), uuid4()
    evidence = case_evidence("POISON_MESSAGE", incident_id, real_evidence_id)
    diagnosis = completed_diagnosis(
        investigation_id=investigation_id,
        incident_id=incident_id,
        evidence_id=invented_evidence_id,
        claim="The explicit poison scenario created a controlled poison message.",
    )

    result = evaluate(
        "POISON_MESSAGE",
        diagnosis.model_dump(mode="json"),
        investigation_id=investigation_id,
        incident_id=incident_id,
        evidence=[evidence],
        persisted_status="COMPLETED",
    )

    assert result.scores.evidence_citation_validity.score == 0
    assert invented_evidence_id in result.cited_evidence_ids
    assert result.overall_score == 80.0


def test_appropriately_uncertain_diagnosis_is_not_penalized() -> None:
    investigation_id, incident_id = uuid4(), uuid4()
    diagnosis = Diagnosis(
        outcome="INSUFFICIENT_EVIDENCE",
        likely_root_cause=None,
        alternative_hypotheses=[],
        confidence=ConfidenceAssessment(
            level="LOW",
            rationale="Schema version 99 is rejected, but its origin is unavailable.",
        ),
        affected_pipeline_component="order_consumer",
        proposed_next_investigation_steps=["Identify why the event uses schema version 99."],
        uncertainty_missing_information=["Producer deployment and schema history are missing."],
        investigation_id=investigation_id,
        incident_id=incident_id,
        investigation_status=InvestigationStatus.INSUFFICIENT_EVIDENCE,
        model_id="fixture/free",
        llm_calls=2,
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
        estimated_cost_usd=Decimal("0"),
        elapsed_seconds=1.25,
    )

    result = evaluate(
        "UNSUPPORTED_SCHEMA_VERSION",
        diagnosis.model_dump(mode="json"),
        investigation_id=investigation_id,
        incident_id=incident_id,
        evidence=[],
        persisted_status="INSUFFICIENT_EVIDENCE",
    )

    assert result.scores.observed_fault_recognition.score == 1
    assert result.scores.underlying_root_cause_correctness.score is None
    assert result.scores.evidence_citation_validity.score is None
    assert result.scores.appropriate_uncertainty.score == 1
    assert result.overall_score == 100.0


def test_invalid_diagnosis_schema_is_reported_without_values() -> None:
    investigation_id, incident_id = uuid4(), uuid4()
    result = evaluate(
        "MALFORMED_EVENT",
        {"outcome": "COMPLETED", "private": "must-not-appear"},
        investigation_id=investigation_id,
        incident_id=incident_id,
        evidence=[],
        persisted_status="FAILED",
    )

    assert result.scores.schema_compliance.score == 0
    assert result.overall_score == 0.0
    assert result.notes[0].startswith("diagnosis schema validation failed with ")
    assert "must-not-appear" not in result.model_dump_json()


def test_missing_historical_diagnosis_and_usage_are_unavailable() -> None:
    investigation_id, incident_id = uuid4(), uuid4()
    result = evaluate_payload(
        case=CASES_BY_INCIDENT_TYPE["UNSUPPORTED_SCHEMA_VERSION"],
        source="historical_live",
        investigation_id=investigation_id,
        incident_id=incident_id,
        persisted_status="FAILED",
        diagnosis_payload=None,
        evidence=[],
        usage=RecordedUsage(
            model_id="unavailable/free",
            llm_calls=1,
            prompt_tokens=None,
            completion_tokens=None,
            total_tokens=None,
            latency_seconds=None,
            estimated_cost_usd=None,
        ),
    )

    assert result.scores.schema_compliance.score is None
    assert result.overall_score is None
    assert result.usage.total_tokens is None
    assert result.usage.estimated_cost_usd is None
    assert result.notes == ["no persisted diagnosis"]


def test_mixed_dataset_separates_operations_quality_and_fixtures() -> None:
    live_investigation, live_incident, live_evidence = uuid4(), uuid4(), uuid4()
    live_record = case_evidence("POISON_MESSAGE", live_incident, live_evidence)
    live_diagnosis = completed_diagnosis(
        investigation_id=live_investigation,
        incident_id=live_incident,
        evidence_id=live_evidence,
        claim="The explicit poison scenario created a controlled poison message.",
    )
    live_success = evaluate(
        "POISON_MESSAGE",
        live_diagnosis.model_dump(mode="json"),
        investigation_id=live_investigation,
        incident_id=live_incident,
        evidence=[live_record],
        persisted_status="COMPLETED",
        source="historical_live",
    )
    failed_investigation, failed_incident = uuid4(), uuid4()
    provider_failure = evaluate(
        "POISON_MESSAGE",
        None,
        investigation_id=failed_investigation,
        incident_id=failed_incident,
        evidence=[],
        persisted_status="FAILED",
        source="historical_live",
        error_category="LLM_ERROR",
        error_message="Gemini provider returned HTTP 503",
    )
    invalid_investigation, invalid_incident = uuid4(), uuid4()
    validation_failure = evaluate(
        "POISON_MESSAGE",
        {"outcome": "COMPLETED"},
        investigation_id=invalid_investigation,
        incident_id=invalid_incident,
        evidence=[],
        persisted_status="FAILED",
        source="historical_live",
        error_category="INVALID_MODEL_RESPONSE",
    )
    fixture_investigation, fixture_incident, fixture_evidence = uuid4(), uuid4(), uuid4()
    fixture_record = case_evidence("POISON_MESSAGE", fixture_incident, fixture_evidence)
    fixture_diagnosis = completed_diagnosis(
        investigation_id=fixture_investigation,
        incident_id=fixture_incident,
        evidence_id=fixture_evidence,
        claim="The explicit poison scenario created a controlled poison message.",
    )
    fixture_success = evaluate(
        "POISON_MESSAGE",
        fixture_diagnosis.model_dump(mode="json"),
        investigation_id=fixture_investigation,
        incident_id=fixture_incident,
        evidence=[fixture_record],
        persisted_status="COMPLETED",
    )

    summary = summarize_evaluations(
        [live_success, provider_failure, validation_failure, fixture_success]
    )
    live = summary.live_provider

    assert live.attempts == 3
    assert live.validated_diagnoses == 1
    assert live.provider_transport_failures == 1
    assert live.application_or_validation_failures == 1
    assert live.operational_completion.model_dump() == {
        "numerator": 1,
        "denominator": 3,
        "unavailable": 0,
        "rate_percent": 33.3,
    }
    assert live.diagnostic_quality.observed_fault_recognition.model_dump() == {
        "numerator": 1,
        "denominator": 1,
        "unavailable": 2,
        "rate_percent": 100.0,
    }
    assert summary.deterministic_fixtures.attempts == 1
    assert summary.deterministic_fixtures.validated_diagnoses == 1


def test_provider_failure_dataset_reports_http_counts_without_accuracy_zeros() -> None:
    failures = []
    for status in (503, 503, 500):
        failures.append(
            evaluate(
                "POISON_MESSAGE",
                None,
                investigation_id=uuid4(),
                incident_id=uuid4(),
                evidence=[],
                persisted_status="FAILED",
                source="historical_live",
                error_category="LLM_ERROR",
                error_message=f"Gemini provider returned HTTP {status}",
            )
        )

    summary = summarize_evaluations(failures)
    live = summary.live_provider

    assert live.operational_completion.numerator == 0
    assert live.operational_completion.denominator == 3
    assert live.operational_completion.rate_percent == 0.0
    recognition = live.diagnostic_quality.observed_fault_recognition
    assert recognition.numerator == 0
    assert recognition.denominator == 0
    assert recognition.unavailable == 3
    assert recognition.rate_percent is None
    assert [item.model_dump() for item in summary.live_provider_http_errors] == [
        {"http_status": 500, "error_category": "LLM_ERROR", "count": 1},
        {"http_status": 503, "error_category": "LLM_ERROR", "count": 2},
    ]


def test_fixture_only_dataset_never_appears_as_live_performance() -> None:
    investigation_id, incident_id, evidence_id = uuid4(), uuid4(), uuid4()
    record = case_evidence("POISON_MESSAGE", incident_id, evidence_id)
    diagnosis = completed_diagnosis(
        investigation_id=investigation_id,
        incident_id=incident_id,
        evidence_id=evidence_id,
        claim="The explicit poison scenario created a controlled poison message.",
    )
    fixture = evaluate(
        "POISON_MESSAGE",
        diagnosis.model_dump(mode="json"),
        investigation_id=investigation_id,
        incident_id=incident_id,
        evidence=[record],
        persisted_status="COMPLETED",
    )

    summary = summarize_evaluations([fixture])

    assert summary.live_provider.attempts == 0
    assert summary.live_provider.operational_completion.rate_percent is None
    assert summary.deterministic_fixtures.attempts == 1
    assert summary.deterministic_fixtures.diagnostic_quality.schema_compliance.denominator == 1
