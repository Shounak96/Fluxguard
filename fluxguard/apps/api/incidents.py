from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from fluxguard.packages.common.config import get_settings
from fluxguard.packages.common.db import get_session
from fluxguard.packages.common.incidents import analyze_impact, evidence_view, incident_view
from fluxguard.packages.common.models import (
    Incident,
    IncidentEvidence,
    IncidentSeverity,
    IncidentStatus,
)
from fluxguard.packages.contracts.incidents import (
    EvidenceListResponse,
    ImpactView,
    IncidentListResponse,
    IncidentView,
)

router = APIRouter(prefix="/api/v1/incidents", tags=["incidents"])


def require_incident_api() -> None:
    settings = get_settings()
    if settings.environment.lower() != "development" or not settings.incident_api_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


def _incident_or_404(session: Session, incident_id: UUID) -> Incident:
    incident = session.get(Incident, incident_id)
    if incident is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="incident not found")
    return incident


@router.get("", response_model=IncidentListResponse, dependencies=[Depends(require_incident_api)])
def list_incidents(
    session: Annotated[Session, Depends(get_session)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    incident_status: IncidentStatus | None = None,
    severity: IncidentSeverity | None = None,
    incident_type: str | None = None,
) -> IncidentListResponse:
    conditions = []
    if incident_status:
        conditions.append(Incident.status == incident_status.value)
    if severity:
        conditions.append(Incident.severity == severity.value)
    if incident_type:
        conditions.append(Incident.incident_type == incident_type)
    query = select(Incident).where(*conditions)
    total = session.scalar(select(func.count()).select_from(query.subquery())) or 0
    rows = session.scalars(
        query.order_by(Incident.last_seen_at.desc()).limit(limit).offset(offset)
    ).all()
    return IncidentListResponse(
        data=[incident_view(row) for row in rows], total=total, limit=limit, offset=offset
    )


@router.get(
    "/{incident_id}", response_model=IncidentView, dependencies=[Depends(require_incident_api)]
)
def get_incident(
    incident_id: UUID, session: Annotated[Session, Depends(get_session)]
) -> IncidentView:
    return incident_view(_incident_or_404(session, incident_id))


@router.get(
    "/{incident_id}/evidence",
    response_model=EvidenceListResponse,
    dependencies=[Depends(require_incident_api)],
)
def get_evidence(
    incident_id: UUID,
    session: Annotated[Session, Depends(get_session)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> EvidenceListResponse:
    _incident_or_404(session, incident_id)
    condition = IncidentEvidence.incident_id == incident_id
    total = session.scalar(select(func.count()).select_from(IncidentEvidence).where(condition)) or 0
    rows = session.scalars(
        select(IncidentEvidence)
        .where(condition)
        .order_by(IncidentEvidence.observed_at)
        .limit(limit)
        .offset(offset)
    ).all()
    return EvidenceListResponse(
        data=[evidence_view(row) for row in rows], total=total, limit=limit, offset=offset
    )


@router.get(
    "/{incident_id}/impact",
    response_model=ImpactView,
    dependencies=[Depends(require_incident_api)],
)
def get_impact(incident_id: UUID, session: Annotated[Session, Depends(get_session)]) -> ImpactView:
    return analyze_impact(session, _incident_or_404(session, incident_id))
