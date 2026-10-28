from collections.abc import Callable
from itertools import pairwise
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from fluxguard.packages.common.incidents import analyze_impact, evidence_view, incident_view
from fluxguard.packages.common.models import (
    Incident,
    IncidentEvidence,
    OrderProjection,
    OutboxEvent,
    ProcessedEvent,
    QuarantinedEvent,
)
from fluxguard.packages.contracts.investigations import (
    ApprovedToolName,
    EventProcessingStatus,
    EventProcessingToolOutput,
    EvidenceToolOutput,
    ImpactToolOutput,
    IncidentToolOutput,
    PipelineTopologyToolOutput,
    RelatedIncidentContext,
    RelatedIncidentsToolOutput,
    ToolCallRecord,
)


class IncidentToolInput(BaseModel):
    incident_id: UUID


class EvidenceToolInput(IncidentToolInput):
    limit: int = Field(default=50, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class RelatedIncidentsToolInput(IncidentToolInput):
    limit: int = Field(default=10, ge=1, le=20)


def _incident(session: Session, incident_id: UUID) -> Incident:
    item = session.get(Incident, incident_id)
    if item is None:
        raise LookupError("incident not found")
    return item


def get_incident(session: Session, tool_input: IncidentToolInput) -> IncidentToolOutput:
    return IncidentToolOutput(incident=incident_view(_incident(session, tool_input.incident_id)))


def list_incident_evidence(session: Session, tool_input: EvidenceToolInput) -> EvidenceToolOutput:
    _incident(session, tool_input.incident_id)
    condition = IncidentEvidence.incident_id == tool_input.incident_id
    total = session.scalar(select(func.count()).select_from(IncidentEvidence).where(condition)) or 0
    rows = session.scalars(
        select(IncidentEvidence)
        .where(condition)
        .order_by(IncidentEvidence.observed_at)
        .limit(tool_input.limit)
        .offset(tool_input.offset)
    ).all()
    return EvidenceToolOutput(
        evidence=[evidence_view(row) for row in rows],
        total=total,
        truncated=tool_input.offset + len(rows) < total,
    )


def get_pipeline_impact(session: Session, tool_input: IncidentToolInput) -> ImpactToolOutput:
    incident = _incident(session, tool_input.incident_id)
    return ImpactToolOutput(impact=analyze_impact(session, incident))


def get_event_processing_status(
    session: Session, tool_input: IncidentToolInput
) -> EventProcessingToolOutput:
    evidence = session.scalars(
        select(IncidentEvidence).where(IncidentEvidence.incident_id == tool_input.incident_id)
    ).all()
    event_ids = sorted({item.event_id for item in evidence if item.event_id}, key=str)
    results: list[EventProcessingStatus] = []
    for event_id in event_ids[:50]:
        processed = session.get(ProcessedEvent, event_id)
        outbox = session.get(OutboxEvent, event_id)
        projection_exists = (
            session.scalar(
                select(func.count())
                .select_from(OrderProjection)
                .where(OrderProjection.source_event_id == event_id)
            )
            or 0
        ) > 0
        coordinates = [
            (item.kafka_topic, item.kafka_partition, item.kafka_offset)
            for item in evidence
            if item.event_id == event_id and item.kafka_topic is not None
        ]
        quarantined = any(
            (
                session.scalar(
                    select(func.count())
                    .select_from(QuarantinedEvent)
                    .where(
                        QuarantinedEvent.topic == topic,
                        QuarantinedEvent.partition == partition,
                        QuarantinedEvent.offset == offset,
                    )
                )
                or 0
            )
            > 0
            for topic, partition, offset in coordinates
        )
        results.append(
            EventProcessingStatus(
                event_id=event_id,
                processed=processed is not None,
                delivery_count=processed.delivery_count if processed else None,
                projection_exists=projection_exists,
                outbox_published=outbox.published_at is not None if outbox else None,
                quarantined=quarantined,
            )
        )
    unavailable = [] if event_ids else ["No event IDs are present in incident evidence"]
    return EventProcessingToolOutput(events=results, unavailable=unavailable)


def get_related_incidents(
    session: Session, tool_input: RelatedIncidentsToolInput
) -> RelatedIncidentsToolOutput:
    current = _incident(session, tool_input.incident_id)
    query = (
        select(Incident)
        .where(
            Incident.incident_id != current.incident_id,
            Incident.source_component == current.source_component,
            or_(
                Incident.incident_type == current.incident_type,
                Incident.error_code == current.error_code,
            ),
        )
        .order_by(Incident.last_seen_at.desc())
    )
    rows = session.scalars(query.limit(tool_input.limit + 1)).all()
    contexts = []
    for row in rows[: tool_input.limit]:
        evidence_ids = session.scalars(
            select(IncidentEvidence.evidence_id)
            .where(IncidentEvidence.incident_id == row.incident_id)
            .order_by(IncidentEvidence.observed_at)
            .limit(20)
        ).all()
        contexts.append(
            RelatedIncidentContext(incident=incident_view(row), evidence_ids=list(evidence_ids))
        )
    return RelatedIncidentsToolOutput(
        incidents=contexts,
        truncated=len(rows) > tool_input.limit,
    )


def get_pipeline_topology(
    _session: Session, _tool_input: IncidentToolInput
) -> PipelineTopologyToolOutput:
    stages = ["API", "Outbox", "Redpanda", "Consumer", "Projection"]
    return PipelineTopologyToolOutput(
        stages=stages,
        edges=[{"from": left, "to": right} for left, right in pairwise(stages)],
        guarantees=[
            "Order and outbox event commit atomically",
            "Publication is at least once after broker acknowledgement",
            "Consumer business effects are idempotent by event ID",
            "Kafka offsets advance only after durable processing or quarantine",
        ],
    )


ToolFunction = Callable[[Session, BaseModel], BaseModel]


class ReadOnlyToolRegistry:
    """Closed registry: the model selects names but cannot provide executable operations."""

    def __init__(self, *, evidence_limit: int = 50) -> None:
        self.evidence_limit = min(max(evidence_limit, 1), 100)
        self._tools: dict[str, tuple[type[BaseModel], ToolFunction]] = {
            "get_incident": (IncidentToolInput, get_incident),
            "list_incident_evidence": (EvidenceToolInput, list_incident_evidence),
            "get_pipeline_impact": (IncidentToolInput, get_pipeline_impact),
            "get_event_processing_status": (IncidentToolInput, get_event_processing_status),
            "get_related_incidents": (RelatedIncidentsToolInput, get_related_incidents),
            "get_pipeline_topology": (IncidentToolInput, get_pipeline_topology),
        }

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._tools)

    def execute(
        self, session: Session, name: ApprovedToolName, incident_id: UUID
    ) -> tuple[BaseModel, ToolCallRecord]:
        if name not in self._tools:
            raise ValueError("tool is not approved")
        input_type, function = self._tools[name]
        values: dict[str, Any] = {"incident_id": incident_id}
        if input_type is EvidenceToolInput:
            values["limit"] = self.evidence_limit
        tool_input = input_type.model_validate(values)
        output = function(session, tool_input)
        record = ToolCallRecord(
            tool=name,
            input=tool_input.model_dump(mode="json"),
            output=output.model_dump(mode="json"),
        )
        return output, record
