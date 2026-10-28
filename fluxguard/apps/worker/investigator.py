import time

from fluxguard.packages.common.config import Settings, get_settings
from fluxguard.packages.common.db import SessionLocal
from fluxguard.packages.common.investigation_agent import InvestigationAgent
from fluxguard.packages.common.investigation_tools import ReadOnlyToolRegistry
from fluxguard.packages.common.investigations import (
    claim_pending_investigation,
    execute_investigation,
    recover_interrupted_investigations,
)
from fluxguard.packages.common.llm import GeminiLLM, InvestigationLLM, OpenRouterLLM
from fluxguard.packages.common.logging import configure_logging, get_logger


def build_llm(settings: Settings, *, client=None) -> InvestigationLLM:
    if settings.llm_provider == "gemini":
        if settings.gemini_api_key is None:
            raise RuntimeError("GEMINI_API_KEY is not configured")
        return GeminiLLM(
            api_key=settings.gemini_api_key.get_secret_value(),
            model_id=settings.gemini_model_id,
            timeout_seconds=settings.investigation_request_timeout_seconds,
            max_retries=settings.investigation_max_retries,
            allow_paid_models=settings.investigation_allow_paid_models,
            client=client,
        )
    if settings.openrouter_api_key is None:
        raise RuntimeError("OPENROUTER_API_KEY is not configured")
    return OpenRouterLLM(
        api_key=settings.openrouter_api_key.get_secret_value(),
        model_id=settings.investigation_model_id,
        base_url=settings.openrouter_base_url,
        timeout_seconds=settings.investigation_request_timeout_seconds,
        max_retries=settings.investigation_max_retries,
        allow_paid_models=settings.investigation_allow_paid_models,
        client=client,
    )


def build_agent() -> InvestigationAgent:
    settings = get_settings()
    llm = build_llm(settings)
    return InvestigationAgent(
        session_factory=SessionLocal,
        llm=llm,
        registry=ReadOnlyToolRegistry(evidence_limit=settings.investigation_evidence_limit),
        max_llm_calls=settings.investigation_max_llm_calls,
        max_iterations=settings.investigation_max_iterations,
    )


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger(__name__)
    recovered = recover_interrupted_investigations(SessionLocal)
    logger.info("investigation_worker_started", recovered_interrupted=recovered)
    agent = None
    while True:
        investigation_id = claim_pending_investigation(SessionLocal)
        if investigation_id is None:
            time.sleep(settings.investigation_poll_interval_seconds)
            continue
        try:
            agent = agent or build_agent()
        except Exception as exc:
            logger.error(
                "investigation_worker_configuration_error",
                investigation_id=str(investigation_id),
                error_type=type(exc).__name__,
            )

            # A deliberately unavailable provider is persisted as a bounded failure.
            class UnavailableAgent:
                def investigate(self, *_args, **_kwargs):
                    raise RuntimeError("model provider is not configured")

            execute_investigation(SessionLocal, UnavailableAgent(), investigation_id)  # type: ignore[arg-type]
            continue
        status = execute_investigation(SessionLocal, agent, investigation_id)
        logger.info(
            "investigation_completed",
            investigation_id=str(investigation_id),
            investigation_status=status.value,
        )


if __name__ == "__main__":
    main()
