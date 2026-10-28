from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from fluxguard.apps.api.incidents import router as incidents_router
from fluxguard.apps.api.investigations import router as investigations_router
from fluxguard.apps.api.service import IdempotencyConflictError, create_order
from fluxguard.packages.common.config import get_settings
from fluxguard.packages.common.db import get_session
from fluxguard.packages.common.logging import (
    configure_logging,
    correlation_id_var,
    get_logger,
)
from fluxguard.packages.common.metrics import counters
from fluxguard.packages.contracts.orders import OrderCreateRequest, OrderResponse

settings = get_settings()
configure_logging(settings.log_level)
logger = get_logger(__name__)


def create_app(session_dependency: Callable = get_session) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        logger.info("api_started", environment=settings.environment)
        yield
        logger.info("api_stopped")

    app = FastAPI(title="FluxGuard Order API", version="0.1.0", lifespan=lifespan)
    app.include_router(incidents_router)
    app.include_router(investigations_router)

    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        supplied = request.headers.get("X-Correlation-ID")
        correlation_id = supplied[:128] if supplied else str(uuid4())
        token = correlation_id_var.set(correlation_id)
        try:
            response = await call_next(request)
            response.headers["X-Correlation-ID"] = correlation_id
            return response
        finally:
            correlation_id_var.reset(token)

    @app.exception_handler(IdempotencyConflictError)
    async def idempotency_conflict(_request: Request, exc: IdempotencyConflictError):
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "error": {
                    "code": "idempotency_conflict",
                    "message": str(exc),
                }
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, exc: RequestValidationError):
        details = [
            {"location": list(error["loc"]), "message": error["msg"], "type": error["type"]}
            for error in exc.errors()
        ]
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "error": {
                    "code": "validation_error",
                    "message": "request validation failed",
                    "details": details,
                }
            },
        )

    @app.post(
        "/api/v1/orders",
        response_model=OrderResponse,
        status_code=status.HTTP_201_CREATED,
    )
    def submit_order(
        request: OrderCreateRequest,
        session: Annotated[Session, Depends(session_dependency)],
        idempotency_key: Annotated[
            str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
        ],
    ) -> OrderResponse:
        order, replayed = create_order(session, request, idempotency_key)
        counters.increment("orders_idempotent_replays" if replayed else "orders_created")
        logger.info(
            "order_submission_completed",
            order_id=str(order.order_id),
            idempotent_replay=replayed,
        )
        return OrderResponse(
            data=order,
            meta={"status": "replayed" if replayed else "accepted"},
        )

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    def ready(session: Annotated[Session, Depends(session_dependency)]) -> dict[str, str]:
        session.execute(text("SELECT 1"))
        return {"status": "ready"}

    @app.get("/metrics")
    def metrics() -> dict[str, dict[str, int]]:
        return {"counters": counters.snapshot()}

    return app


app = create_app()
