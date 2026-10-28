import os
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from fluxguard.packages.common.incidents import Detection, record_incident
from fluxguard.packages.common.investigation_agent import InvestigationAgent
from fluxguard.packages.common.investigation_tools import ReadOnlyToolRegistry
from fluxguard.packages.common.investigations import (
    claim_pending_investigation,
    enqueue_investigation,
    execute_investigation,
)
from fluxguard.packages.common.llm import LLMResponse, LLMUsage
from fluxguard.packages.common.models import (
    IncidentEvidence,
    IncidentSeverity,
    InvestigationRun,
    InvestigationStatus,
)
from fluxguard.packages.contracts.investigations import (
    ConfidenceAssessment,
    DraftDiagnosis,
    EvidenceHypothesis,
    ToolPlan,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1",
        reason="set RUN_INTEGRATION=1 with migrated PostgreSQL",
    ),
]


class PostgresFakeLLM:
    model_id = "integration/fake:free"

    def __init__(self, evidence_id):
        self.evidence_id = evidence_id
        self.calls = 0

    def complete(self, _prompt, response_model):
        self.calls += 1
        if response_model is ToolPlan:
            value = ToolPlan(
                tools=["get_pipeline_impact", "get_event_processing_status"],
                competing_hypotheses=["producer emitted bad JSON", "payload was truncated"],
                focus="identify the malformed stage",
            )
        else:
            value = DraftDiagnosis(
                outcome="COMPLETED",
                likely_root_cause=EvidenceHypothesis(
                    claim="The consumer received malformed JSON.",
                    rationale="Durable quarantine evidence classifies the record.",
                    supporting_evidence_ids=[self.evidence_id],
                ),
                alternative_hypotheses=[],
                confidence=ConfidenceAssessment(
                    level="MEDIUM", rationale="The producer identity is unavailable."
                ),
                affected_pipeline_component="order_consumer",
                proposed_next_investigation_steps=["Inspect producer logs."],
                uncertainty_missing_information=["Producer identity is unavailable."],
            )
        return LLMResponse(
            value=response_model.model_validate(value.model_dump()),
            model_id=self.model_id,
            usage=LLMUsage(20, 10, 30, Decimal("0")),
        )


def test_real_postgres_persists_grounded_investigation() -> None:
    factory = sessionmaker(bind=create_engine(os.environ["DATABASE_URL"]), expire_on_commit=False)
    with factory() as session, session.begin():
        incident, _ = record_incident(
            session,
            Detection(
                incident_type="MALFORMED_EVENT",
                severity=IncidentSeverity.HIGH,
                error_code="MALFORMED_JSON",
                summary="malformed test event",
                source_component="investigation_integration",
                fingerprint_scope={"scenario_id": str(uuid4())},
                evidence_type="QUARANTINED_EVENT",
                evidence_payload={"scenario_id": str(uuid4())},
                topic="orders.created.v1",
                partition=0,
                offset=50_000,
            ),
        )
        evidence_id = session.scalar(
            select(IncidentEvidence.evidence_id).where(
                IncidentEvidence.incident_id == incident.incident_id
            )
        )
    with factory() as session:
        accepted = enqueue_investigation(session, incident.incident_id, PostgresFakeLLM.model_id)
    assert claim_pending_investigation(factory) == accepted.investigation_id
    fake = PostgresFakeLLM(evidence_id)
    agent = InvestigationAgent(
        session_factory=factory,
        llm=fake,
        registry=ReadOnlyToolRegistry(evidence_limit=10),
        max_llm_calls=4,
        max_iterations=2,
    )
    assert (
        execute_investigation(factory, agent, accepted.investigation_id)
        == InvestigationStatus.COMPLETED
    )
    with factory() as session:
        stored = session.get(InvestigationRun, accepted.investigation_id)
        assert stored.status == InvestigationStatus.COMPLETED.value
        assert stored.diagnosis["likely_root_cause"]["supporting_evidence_ids"] == [
            str(evidence_id)
        ]
        assert stored.llm_call_count == 2
        assert stored.total_tokens == 60
        assert len(stored.tool_call_history) >= 3
