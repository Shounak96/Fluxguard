from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from fluxguard.packages.common.config import get_settings
from fluxguard.packages.common.db import get_session
from fluxguard.packages.common.recoveries import get_recovery_plan_review
from fluxguard.packages.common.recovery_approvals import (
    DisabledRecoveryAuthorizer,
    RecoveryApprovalConflictError,
    RecoveryApprovalError,
    RecoveryApprovalExpiredError,
    RecoveryApprovalStaleError,
    RecoveryApprovalTransitionError,
    RecoveryAuthorizationError,
    RecoveryAuthorizer,
    approve_recovery_plan,
)
from fluxguard.packages.contracts.recovery import (
    RecoveryApprovalRequest,
    RecoveryApprovalView,
    RecoveryEvidenceReference,
    RecoveryExpectedEffectsView,
    RecoveryLifecycleEventView,
    RecoveryPlanReview,
    RecoveryVerificationView,
)

router = APIRouter(prefix="/api/v1/recovery-plans", tags=["recoveries"])


def require_recovery_review_api() -> None:
    settings = get_settings()
    if settings.environment.lower() != "development" or not settings.recovery_api_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


def get_recovery_authorizer() -> RecoveryAuthorizer:
    return DisabledRecoveryAuthorizer()


def _review_or_404(session: Session, plan_id: UUID) -> RecoveryPlanReview:
    try:
        return get_recovery_plan_review(session, plan_id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="plan not found") from exc


def _approval_http_error(exc: RecoveryApprovalError) -> HTTPException:
    status_code = status.HTTP_409_CONFLICT
    if isinstance(exc, RecoveryAuthorizationError):
        status_code = status.HTTP_403_FORBIDDEN
    elif isinstance(exc, RecoveryApprovalExpiredError):
        status_code = status.HTTP_410_GONE
    elif isinstance(
        exc,
        RecoveryApprovalConflictError
        | RecoveryApprovalStaleError
        | RecoveryApprovalTransitionError,
    ):
        status_code = status.HTTP_409_CONFLICT
    return HTTPException(
        status_code=status_code,
        detail={"code": exc.code, "message": str(exc)},
    )


@router.get(
    "/{plan_id}",
    response_model=RecoveryPlanReview,
    dependencies=[Depends(require_recovery_review_api)],
)
def get_recovery_review(
    plan_id: UUID, session: Annotated[Session, Depends(get_session)]
) -> RecoveryPlanReview:
    return _review_or_404(session, plan_id)


@router.get(
    "/{plan_id}/evidence",
    response_model=list[RecoveryEvidenceReference],
    dependencies=[Depends(require_recovery_review_api)],
)
def get_recovery_evidence(
    plan_id: UUID, session: Annotated[Session, Depends(get_session)]
) -> list[RecoveryEvidenceReference]:
    return list(_review_or_404(session, plan_id).evidence)


@router.get(
    "/{plan_id}/verification/latest",
    response_model=RecoveryVerificationView,
    dependencies=[Depends(require_recovery_review_api)],
)
def get_latest_recovery_verification(
    plan_id: UUID, session: Annotated[Session, Depends(get_session)]
) -> RecoveryVerificationView:
    verification = _review_or_404(session, plan_id).latest_verification
    if verification is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="verification not found")
    return verification


@router.get(
    "/{plan_id}/expected-effects",
    response_model=RecoveryExpectedEffectsView,
    dependencies=[Depends(require_recovery_review_api)],
)
def get_recovery_expected_effects(
    plan_id: UUID, session: Annotated[Session, Depends(get_session)]
) -> RecoveryExpectedEffectsView:
    review = _review_or_404(session, plan_id)
    return RecoveryExpectedEffectsView(
        expected_effect=review.expected_effect,
        declared_write_set=review.declared_write_set,
        postconditions=review.postconditions,
    )


@router.get(
    "/{plan_id}/history",
    response_model=list[RecoveryLifecycleEventView],
    dependencies=[Depends(require_recovery_review_api)],
)
def get_recovery_lifecycle_history(
    plan_id: UUID, session: Annotated[Session, Depends(get_session)]
) -> list[RecoveryLifecycleEventView]:
    return list(_review_or_404(session, plan_id).lifecycle_events)


@router.post(
    "/{plan_id}/approval",
    response_model=RecoveryApprovalView,
    dependencies=[Depends(require_recovery_review_api)],
)
def approve_recovery(
    plan_id: UUID,
    request: RecoveryApprovalRequest,
    session: Annotated[Session, Depends(get_session)],
    authorizer: Annotated[RecoveryAuthorizer, Depends(get_recovery_authorizer)],
) -> RecoveryApprovalView:
    try:
        operator = authorizer.authenticate()
        return approve_recovery_plan(
            session,
            plan_id,
            operator=operator,
            idempotency_key=request.idempotency_key,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="plan not found") from exc
    except RecoveryApprovalError as exc:
        raise _approval_http_error(exc) from exc
