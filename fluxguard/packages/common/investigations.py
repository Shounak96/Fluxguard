from datetime import timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from fluxguard.packages.common.incidents import redact
from fluxguard.packages.common.investigation_agent import InvestigationAgent
from fluxguard.packages.common.llm import LLMError
from fluxguard.packages.common.models import (
    Incident,
    InvestigationRun,
    InvestigationStatus,
)
from fluxguard.packages.common.time import utc_now
from fluxguard.packages.contracts.investigations import (
    Diagnosis,
    InvestigationAccepted,
    InvestigationView,
    ToolCallRecord,
)


def enqueue_investigation(
    session: Session, incident_id: UUID, model_id: str
) -> InvestigationAccepted:
    if session.get(Incident, incident_id) is None:
        raise LookupError("incident not found")
    run = InvestigationRun(
        investigation_id=uuid4(),
        incident_id=incident_id,
        model_id=model_id,
        status=InvestigationStatus.PENDING.value,
        tool_call_history=[],
    )
    session.add(run)
    session.commit()
    return InvestigationAccepted(
        investigation_id=run.investigation_id,
        incident_id=run.incident_id,
        status=InvestigationStatus(run.status),
    )


def investigation_view(run: InvestigationRun) -> InvestigationView:
    return InvestigationView(
        investigation_id=run.investigation_id,
        incident_id=run.incident_id,
        model_id=run.model_id,
        status=InvestigationStatus(run.status),
        created_at=run.created_at,
        started_at=run.started_at,
        completed_at=run.completed_at,
        diagnosis=Diagnosis.model_validate(run.diagnosis) if run.diagnosis else None,
        tool_call_history=[ToolCallRecord.model_validate(item) for item in run.tool_call_history],
        prompt_tokens=run.prompt_tokens,
        completion_tokens=run.completion_tokens,
        total_tokens=run.total_tokens,
        estimated_cost_usd=(
            Decimal(run.estimated_cost_usd) if run.estimated_cost_usd is not None else None
        ),
        llm_call_count=run.llm_call_count,
        error_category=run.error_category,
        error_message=run.error_message,
    )


def claim_pending_investigation(session_factory: sessionmaker[Session]) -> UUID | None:
    with session_factory() as session, session.begin():
        run = session.scalar(
            select(InvestigationRun)
            .where(InvestigationRun.status == InvestigationStatus.PENDING.value)
            .order_by(InvestigationRun.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if run is None:
            return None
        run.status = InvestigationStatus.RUNNING.value
        run.started_at = utc_now()
        return run.investigation_id


def execute_investigation(
    session_factory: sessionmaker[Session],
    agent: InvestigationAgent,
    investigation_id: UUID,
) -> InvestigationStatus:
    with session_factory() as session:
        run = session.get(InvestigationRun, investigation_id)
        if run is None:
            raise LookupError("investigation not found")
        incident_id = run.incident_id
    try:
        result = agent.investigate(investigation_id, incident_id)
    except Exception as exc:
        category = exc.category if isinstance(exc, LLMError) else "INVESTIGATION_ERROR"
        safe = redact({"message": str(exc)})["message"]
        with session_factory() as session, session.begin():
            run = session.get(InvestigationRun, investigation_id)
            run.status = InvestigationStatus.FAILED.value
            run.completed_at = utc_now()
            run.error_category = category
            run.error_message = str(safe)[:500]
            metrics = getattr(agent, "last_run_metrics", {})
            run.model_id = metrics.get("actual_model_id", run.model_id)
            run.llm_call_count = metrics.get("llm_calls", 0)
            run.prompt_tokens = metrics.get("prompt_tokens") or None
            run.completion_tokens = metrics.get("completion_tokens") or None
            run.total_tokens = metrics.get("total_tokens") or None
            run.estimated_cost_usd = metrics.get("estimated_cost_usd")
        return InvestigationStatus.FAILED
    diagnosis = result.diagnosis
    with session_factory() as session, session.begin():
        run = session.get(InvestigationRun, investigation_id)
        run.status = diagnosis.investigation_status.value
        run.completed_at = utc_now()
        run.model_id = diagnosis.model_id
        run.diagnosis = diagnosis.model_dump(mode="json")
        run.tool_call_history = [item.model_dump(mode="json") for item in result.tool_history]
        run.prompt_tokens = diagnosis.prompt_tokens
        run.completion_tokens = diagnosis.completion_tokens
        run.total_tokens = diagnosis.total_tokens
        run.estimated_cost_usd = diagnosis.estimated_cost_usd
        run.llm_call_count = diagnosis.llm_calls
        run.error_category = (
            "EVIDENCE_GROUNDING_FAILED"
            if diagnosis.investigation_status == InvestigationStatus.UNVERIFIED
            else None
        )
    return diagnosis.investigation_status


def recover_interrupted_investigations(
    session_factory: sessionmaker[Session], *, stale_after: timedelta = timedelta(minutes=10)
) -> int:
    cutoff = utc_now() - stale_after
    with session_factory() as session, session.begin():
        runs = session.scalars(
            select(InvestigationRun).where(
                InvestigationRun.status == InvestigationStatus.RUNNING.value,
                InvestigationRun.started_at < cutoff,
            )
        ).all()
        for run in runs:
            run.status = InvestigationStatus.FAILED.value
            run.completed_at = utc_now()
            run.error_category = "WORKER_INTERRUPTED"
            run.error_message = "investigation worker stopped before completion"
        return len(runs)
