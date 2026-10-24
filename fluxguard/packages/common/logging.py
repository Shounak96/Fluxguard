import contextvars
import logging
import sys
from typing import Any

import structlog

correlation_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)
event_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("event_id", default=None)


def _add_context(_logger: Any, _method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    correlation_id = correlation_id_var.get()
    event_id = event_id_var.get()
    if correlation_id:
        event_dict["correlation_id"] = correlation_id
    if event_id:
        event_dict["event_id"] = event_id
    return event_dict


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level.upper())
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _add_context,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str):
    return structlog.get_logger(name)
