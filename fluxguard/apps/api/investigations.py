from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from fluxguard.packages.common.config import get_settings
from fluxguard.packages.common.db import get_session
from fluxguard.packages.common.investigations import enqueue_investigation, investigation_view
from fluxguard.packages.common.models import Incident, InvestigationRun
from fluxguard.packages.contracts.investigations import (
    InvestigationAccepted,
    InvestigationListResponse,
    InvestigationView,
)

router = APIRouter(prefix="/api/v1", tags=["investigations"])


def require_investigation_api() -> None:
    settings = get_settings()
    if settings.environment.lower() != "development" or not settings.investigation_api_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


@router.post(
    "/incidents/{incident_id}/investigations",
    response_model=InvestigationAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_investigation_api)],
)
def create_investigation(
    incident_id: UUID, session: Annotated[Session, Depends(get_session)]
) -> InvestigationAccepted:
    settings = get_settings()
    if not settings.investigation_allow_paid_models and not (
        settings.active_investigation_model_is_free
    ):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="configured investigation model is not approved by the free-model policy",
        )
    if session.get(Incident, incident_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="incident not found")
    return enqueue_investigation(session, incident_id, settings.active_investigation_model_id)


@router.get(
    "/investigations/{investigation_id}",
    response_model=InvestigationView,
    dependencies=[Depends(require_investigation_api)],
)
def get_investigation(
    investigation_id: UUID, session: Annotated[Session, Depends(get_session)]
) -> InvestigationView:
    run = session.get(InvestigationRun, investigation_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="investigation not found")
    return investigation_view(run)


@router.get(
    "/incidents/{incident_id}/investigations",
    response_model=InvestigationListResponse,
    dependencies=[Depends(require_investigation_api)],
)
def list_investigations(
    incident_id: UUID,
    session: Annotated[Session, Depends(get_session)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> InvestigationListResponse:
    if session.get(Incident, incident_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="incident not found")
    condition = InvestigationRun.incident_id == incident_id
    total = session.scalar(select(func.count()).select_from(InvestigationRun).where(condition)) or 0
    rows = session.scalars(
        select(InvestigationRun)
        .where(condition)
        .order_by(InvestigationRun.created_at.desc())
        .limit(limit)
        .offset(offset)
    ).all()
    return InvestigationListResponse(
        data=[investigation_view(row) for row in rows],
        total=total,
        limit=limit,
        offset=offset,
    )
