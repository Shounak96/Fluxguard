from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fluxguard.packages.common.models import InvestigationStatus
from fluxguard.packages.contracts.incidents import EvidenceView, ImpactView, IncidentView

CONFIDENCE_CALIBRATION_NOTICE = (
    "Uncalibrated model assessment; not a statistically validated probability."
)

ApprovedToolName = Literal[
    "get_incident",
    "list_incident_evidence",
    "get_pipeline_impact",
    "get_event_processing_status",
    "get_related_incidents",
    "get_pipeline_topology",
]


class StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ToolPlan(StrictContract):
    tools: list[ApprovedToolName] = Field(min_length=1, max_length=6)
    competing_hypotheses: list[str] = Field(min_length=2, max_length=5)
    focus: str = Field(max_length=500)


class EvidenceHypothesis(StrictContract):
    claim: str = Field(max_length=1000)
    rationale: str = Field(max_length=2000)
    supporting_evidence_ids: list[UUID] = Field(min_length=1, max_length=50)
    contradicting_evidence_ids: list[UUID] = Field(default_factory=list, max_length=50)


class ConfidenceAssessment(StrictContract):
    level: Literal["LOW", "MEDIUM", "HIGH"]
    rationale: str = Field(max_length=1000)
    calibration_notice: Literal[CONFIDENCE_CALIBRATION_NOTICE] = Field(
        default=CONFIDENCE_CALIBRATION_NOTICE,
        json_schema_extra={"readOnly": True},
    )


class DraftDiagnosis(StrictContract):
    outcome: Literal["COMPLETED", "INSUFFICIENT_EVIDENCE"]
    likely_root_cause: EvidenceHypothesis | None
    alternative_hypotheses: list[EvidenceHypothesis] = Field(default_factory=list, max_length=5)
    confidence: ConfidenceAssessment
    affected_pipeline_component: str = Field(max_length=100)
    proposed_next_investigation_steps: list[str] = Field(max_length=10)
    uncertainty_missing_information: list[str] = Field(max_length=10)

    @model_validator(mode="after")
    def validate_outcome_consistency(self):
        if self.outcome == "COMPLETED" and self.likely_root_cause is None:
            raise ValueError("completed diagnosis requires a likely root cause")
        if self.outcome == "INSUFFICIENT_EVIDENCE" and self.likely_root_cause is not None:
            raise ValueError("insufficient-evidence diagnosis cannot assert a root cause")
        return self


class Diagnosis(DraftDiagnosis):
    investigation_id: UUID
    incident_id: UUID
    investigation_status: InvestigationStatus
    model_id: str
    llm_calls: int = Field(ge=0)
    prompt_tokens: int | None = Field(default=None, ge=0)
    completion_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    estimated_cost_usd: Decimal | None = Field(default=None, ge=0)
    elapsed_seconds: float = Field(ge=0)


class IncidentToolOutput(StrictContract):
    incident: IncidentView


class EvidenceToolOutput(StrictContract):
    evidence: list[EvidenceView]
    total: int
    truncated: bool


class ImpactToolOutput(StrictContract):
    impact: ImpactView


class EventProcessingStatus(StrictContract):
    event_id: UUID
    processed: bool
    delivery_count: int | None
    projection_exists: bool
    outbox_published: bool | None
    quarantined: bool


class EventProcessingToolOutput(StrictContract):
    events: list[EventProcessingStatus]
    unavailable: list[str]


class RelatedIncidentContext(StrictContract):
    incident: IncidentView
    evidence_ids: list[UUID]


class RelatedIncidentsToolOutput(StrictContract):
    incidents: list[RelatedIncidentContext]
    truncated: bool


class PipelineTopologyToolOutput(StrictContract):
    stages: list[str]
    edges: list[dict[str, str]]
    guarantees: list[str]


class ToolCallRecord(StrictContract):
    tool: ApprovedToolName
    input: dict[str, Any]
    output: dict[str, Any]


class InvestigationView(StrictContract):
    investigation_id: UUID
    incident_id: UUID
    model_id: str
    status: InvestigationStatus
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    diagnosis: Diagnosis | None
    tool_call_history: list[ToolCallRecord]
    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None
    estimated_cost_usd: Decimal | None
    llm_call_count: int
    error_category: str | None
    error_message: str | None


class InvestigationAccepted(StrictContract):
    investigation_id: UUID
    incident_id: UUID
    status: InvestigationStatus


class InvestigationListResponse(StrictContract):
    data: list[InvestigationView]
    total: int
    limit: int = Field(ge=1, le=100)
    offset: int = Field(ge=0)
