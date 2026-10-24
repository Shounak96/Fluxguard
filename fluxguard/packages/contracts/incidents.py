from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from fluxguard.packages.common.models import IncidentSeverity, IncidentStatus


class IncidentView(BaseModel):
    incident_id: UUID
    incident_type: str
    severity: IncidentSeverity
    status: IncidentStatus
    source_component: str
    detected_at: datetime
    updated_at: datetime
    first_seen_at: datetime
    last_seen_at: datetime
    occurrence_count: int
    error_code: str
    summary: str
    fingerprint: str
    metadata: dict[str, Any]


class EvidenceView(BaseModel):
    evidence_id: UUID
    incident_id: UUID
    evidence_type: str
    source: str
    event_id: UUID | None
    kafka_topic: str | None
    kafka_partition: int | None
    kafka_offset: int | None
    observed_at: datetime
    payload: dict[str, Any]


class IncidentListResponse(BaseModel):
    data: list[IncidentView]
    total: int
    limit: int = Field(ge=1, le=100)
    offset: int = Field(ge=0)


class EvidenceListResponse(BaseModel):
    data: list[EvidenceView]
    total: int
    limit: int = Field(ge=1, le=100)
    offset: int = Field(ge=0)


class ImpactView(BaseModel):
    incident_id: UUID
    pipeline: list[str]
    failed_stage: str
    affected_event_ids: list[UUID]
    kafka_positions: list[dict[str, Any]]
    quarantine: dict[str, Any]
    downstream_projection: dict[str, Any]
    event_count: int
    earliest_occurrence: datetime
    latest_occurrence: datetime
    schema_versions: list[int]
    evidence_ids: list[UUID]
    unavailable: list[str]
