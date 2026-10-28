import json
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Generic, Protocol, TypeVar

import httpx
from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types
from pydantic import BaseModel, ValidationError

from fluxguard.packages.common.config import GEMINI_FREE_MODEL_IDS

T = TypeVar("T", bound=BaseModel)
NEMOTRON_35_SUPER_VL_FREE = "nvidia/nemotron-3.5-super-vl:free"
NATIVE_JSON_SCHEMA_MODELS = frozenset({NEMOTRON_35_SUPER_VL_FREE})
REASONING_DISABLED_MODELS = frozenset({NEMOTRON_35_SUPER_VL_FREE})
DEFAULT_MAX_OUTPUT_TOKENS = 1800
# A diagnosis may contain a root cause plus five alternatives. This remains bounded while
# providing headroom for their JSON fields after reasoning is explicitly disabled.
MODEL_MAX_OUTPUT_TOKENS = {NEMOTRON_35_SUPER_VL_FREE: 4096}
GEMINI_MAX_OUTPUT_TOKENS = 4096
# Gemini's constrained decoder can reject otherwise valid JSON Schema when nested array
# bounds create too many grammar states. These hints are enforced by Pydantic after the
# response, so omit them (and other nonessential annotations) from the provider schema.
GEMINI_PROVIDER_OMITTED_SCHEMA_KEYS = frozenset(
    {
        "additionalProperties",
        "default",
        "description",
        "format",
        "maxItems",
        "maxLength",
        "minItems",
        "minLength",
        "title",
    }
)
JSON_FENCE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.IGNORECASE | re.DOTALL)


class LLMError(RuntimeError):
    category = "LLM_ERROR"


class LLMTimeoutError(LLMError):
    category = "MODEL_TIMEOUT"


class LLMRateLimitError(LLMError):
    category = "MODEL_RATE_LIMIT"


class LLMInvalidResponseError(LLMError):
    category = "INVALID_MODEL_RESPONSE"


class LLMInvalidRequestError(LLMError):
    category = "MODEL_INVALID_REQUEST"


class LLMCallLimitError(LLMError):
    category = "MODEL_CALL_LIMIT"


class PaidModelRejectedError(LLMError):
    category = "PAID_MODEL_REJECTED"


@dataclass(frozen=True)
class LLMUsage:
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    estimated_cost_usd: Decimal | None = None


@dataclass(frozen=True)
class LLMResponse(Generic[T]):
    value: T
    model_id: str
    usage: LLMUsage


class InvestigationLLM(Protocol):
    model_id: str

    def complete(self, prompt: str, response_model: type[T]) -> LLMResponse[T]: ...


def _extract_json(content: str) -> dict:
    cleaned = content.strip().lstrip("\ufeff")
    fenced = JSON_FENCE.findall(cleaned)
    if fenced:
        if len(fenced) != 1:
            raise LLMInvalidResponseError("model response contained multiple JSON blocks")
        cleaned = fenced[0]
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise LLMInvalidResponseError("model response was not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise LLMInvalidResponseError("model response must be a JSON object")
    return parsed


def _message_content(message: dict) -> str | None:
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [
            part.get("text")
            for part in content
            if isinstance(part, dict)
            and part.get("type") in {"text", "output_text"}
            and isinstance(part.get("text"), str)
        ]
        return "".join(parts) if parts else None
    return None


def _structured_content(choice: dict) -> str:
    finish_reason = choice.get("finish_reason")
    if finish_reason in {"length", "max_tokens"}:
        raise LLMInvalidResponseError("model output was truncated before completion")
    if finish_reason == "content_filter":
        raise LLMInvalidResponseError("model output was blocked by a content filter")
    message = choice.get("message")
    if not isinstance(message, dict):
        raise LLMInvalidResponseError("model response did not contain a message")
    content = _message_content(message)
    if content and content.strip():
        return content
    tool_calls = message.get("tool_calls")
    if isinstance(tool_calls, list) and len(tool_calls) == 1:
        function = tool_calls[0].get("function") if isinstance(tool_calls[0], dict) else None
        arguments = function.get("arguments") if isinstance(function, dict) else None
        if isinstance(arguments, str):
            return arguments
        if isinstance(arguments, dict):
            return json.dumps(arguments)
    if message.get("reasoning") or message.get("reasoning_details"):
        raise LLMInvalidResponseError("model returned reasoning without final structured content")
    raise LLMInvalidResponseError("model response contained no structured content")


def _model_input_schema(response_model: type[T]) -> dict:
    """Build a strict model-facing schema without application-owned fields."""
    schema = response_model.model_json_schema()

    def normalize(node) -> None:
        if isinstance(node, list):
            for item in node:
                normalize(item)
            return
        if not isinstance(node, dict):
            return
        properties = node.get("properties")
        if isinstance(properties, dict):
            for name in list(properties):
                field_schema = properties[name]
                if isinstance(field_schema, dict) and field_schema.get("readOnly") is True:
                    del properties[name]
                    continue
                normalize(field_schema)
            node["required"] = list(properties)
        for key, value in node.items():
            if key != "properties":
                normalize(value)

    normalize(schema)
    return schema


def _gemini_input_schema(response_model: type[T]) -> dict:
    """Build a low-complexity Gemini schema; Pydantic remains authoritative."""
    schema = _model_input_schema(response_model)

    def simplify(node) -> None:
        if isinstance(node, list):
            for item in node:
                simplify(item)
            return
        if not isinstance(node, dict):
            return
        for key in list(node):
            if key in GEMINI_PROVIDER_OMITTED_SCHEMA_KEYS:
                del node[key]
            else:
                simplify(node[key])

    simplify(schema)
    return schema


def _gemini_invalid_request_message(error: genai_errors.APIError) -> str:
    """Retain only safe provider classification and parameter paths."""
    raw_status = getattr(error, "status", None)
    status = (
        re.sub(r"[^A-Z0-9_]", "_", raw_status.upper())[:64]
        if isinstance(raw_status, str) and raw_status
        else None
    )
    paths: list[str] = []

    def collect_paths(node) -> None:
        if isinstance(node, list):
            for item in node:
                collect_paths(item)
            return
        if not isinstance(node, dict):
            return
        for key, value in node.items():
            if key in {"field", "fieldPath", "parameter"} and isinstance(value, str):
                safe = re.sub(r"[^A-Za-z0-9_.$\[\]-]", "_", value)[:200]
                if safe and safe not in paths:
                    paths.append(safe)
            else:
                collect_paths(value)

    collect_paths(getattr(error, "details", None))
    raw_message = getattr(error, "message", None)
    lowered = raw_message.lower() if isinstance(raw_message, str) else ""
    reason = None
    if "too many states" in lowered or "too much branching" in lowered:
        reason = "schema_complexity"
    elif "response_schema" in lowered or "response schema" in lowered:
        reason = "invalid_schema"
    suffixes = []
    if status:
        suffixes.append(f"status={status}")
    if reason:
        suffixes.append(f"reason={reason}")
    if paths:
        suffixes.append(f"paths={','.join(paths[:4])}")
    suffix = f" ({'; '.join(suffixes)})" if suffixes else ""
    return f"Gemini provider rejected request HTTP 400{suffix}"


def _validation_error_message(error: ValidationError, response_model: type[T], schema: dict) -> str:
    """Return field paths and error types without echoing model-provided values."""
    allowed_fields: set[str] = set()

    def collect_fields(node) -> None:
        if isinstance(node, list):
            for item in node:
                collect_fields(item)
            return
        if not isinstance(node, dict):
            return
        properties = node.get("properties")
        if isinstance(properties, dict):
            allowed_fields.update(properties)
        for value in node.values():
            collect_fields(value)

    collect_fields(schema)
    errors = error.errors(include_url=False, include_context=False, include_input=False)
    details: list[str] = []
    for item in errors[:8]:
        location = []
        for part in item.get("loc", ()):
            if isinstance(part, int):
                location.append(f"[{part}]")
            elif isinstance(part, str) and part in allowed_fields:
                location.append(part)
            else:
                location.append("<unknown-field>")
        path = ".".join(location).replace(".[", "[") if location else "<model>"
        error_type = re.sub(r"[^a-z0-9_.-]", "_", str(item.get("type", "invalid")))
        details.append(f"{path}={error_type}")
    if len(errors) > len(details):
        details.append(f"...+{len(errors) - len(details)} more")
    model_name = re.sub(r"[^A-Za-z0-9_.-]", "_", response_model.__name__)
    return (
        f"model response failed {model_name} validation "
        f"({len(errors)} error(s)): {'; '.join(details)}"
    )


class OpenRouterLLM:
    def __init__(
        self,
        *,
        api_key: str,
        model_id: str,
        base_url: str,
        timeout_seconds: float,
        max_retries: int,
        allow_paid_models: bool = False,
        supports_json_schema: bool | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("OPENROUTER_API_KEY is required")
        if not allow_paid_models and not model_id.endswith(":free"):
            raise PaidModelRejectedError("paid models are disabled")
        self.model_id = model_id
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.supports_json_schema = (
            model_id in NATIVE_JSON_SCHEMA_MODELS
            if supports_json_schema is None
            else supports_json_schema
        )
        self._client = client or httpx.Client(timeout=timeout_seconds)
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def complete(self, prompt: str, response_model: type[T]) -> LLMResponse[T]:
        validation_schema = response_model.model_json_schema()
        schema = _model_input_schema(response_model)
        request = {
            "model": self.model_id,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are FluxGuard's read-only incident investigator. Return only one "
                        "JSON object matching the supplied schema. Include every property; use "
                        "an empty array only where the schema allows it. Copy evidence IDs "
                        "exactly from supplied evidence and never invent them. Do not include "
                        "application execution metadata, prose, analysis, or Markdown fences."
                    ),
                },
                {
                    "role": "user",
                    "content": f"{prompt}\n\nRequired JSON schema:\n{json.dumps(schema)}",
                },
            ],
            "temperature": 0,
            "max_tokens": MODEL_MAX_OUTPUT_TOKENS.get(self.model_id, DEFAULT_MAX_OUTPUT_TOKENS),
        }
        if self.model_id in REASONING_DISABLED_MODELS:
            request["reasoning_effort"] = "none"
        if self.supports_json_schema:
            request["provider"] = {"require_parameters": True}
            request["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": response_model.__name__,
                    "strict": True,
                    "schema": schema,
                },
            }
        response: httpx.Response | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.post(
                    f"{self.base_url}/chat/completions", headers=self._headers, json=request
                )
            except httpx.TimeoutException as exc:
                if attempt >= self.max_retries:
                    raise LLMTimeoutError("model request timed out") from exc
                time.sleep(0.5 * (2**attempt))
                continue
            except httpx.RequestError as exc:
                if attempt >= self.max_retries:
                    raise LLMError("model provider is unavailable") from exc
                time.sleep(0.5 * (2**attempt))
                continue
            if response.status_code == 429:
                if attempt >= self.max_retries:
                    raise LLMRateLimitError("model rate limit persisted after bounded retries")
                retry_after = response.headers.get("retry-after")
                delay = min(float(retry_after), 5.0) if retry_after else 0.5 * (2**attempt)
                time.sleep(delay)
                continue
            if response.status_code >= 500 and attempt < self.max_retries:
                time.sleep(0.5 * (2**attempt))
                continue
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise LLMError(f"model provider returned HTTP {response.status_code}") from exc
            break
        if response is None:
            raise LLMError("model provider returned no response")
        try:
            payload = response.json()
            choice = payload["choices"][0]
            parsed = _extract_json(_structured_content(choice))
            value = response_model.model_validate_json(json.dumps(parsed), strict=True)
        except LLMInvalidResponseError:
            raise
        except ValidationError as exc:
            raise LLMInvalidResponseError(
                _validation_error_message(exc, response_model, validation_schema)
            ) from exc
        except (
            AttributeError,
            KeyError,
            IndexError,
            TypeError,
            ValueError,
        ) as exc:
            raise LLMInvalidResponseError("model response failed schema validation") from exc
        usage = payload.get("usage") or {}
        prompt_tokens = usage.get("prompt_tokens")
        completion_tokens = usage.get("completion_tokens")
        total_tokens = usage.get("total_tokens")
        return LLMResponse(
            value=value,
            model_id=str(payload.get("model") or self.model_id),
            usage=LLMUsage(
                prompt_tokens=prompt_tokens if isinstance(prompt_tokens, int) else None,
                completion_tokens=(
                    completion_tokens if isinstance(completion_tokens, int) else None
                ),
                total_tokens=total_tokens if isinstance(total_tokens, int) else None,
                estimated_cost_usd=Decimal("0") if self.model_id.endswith(":free") else None,
            ),
        )


class GeminiLLM:
    def __init__(
        self,
        *,
        api_key: str,
        model_id: str,
        timeout_seconds: float,
        max_retries: int,
        allow_paid_models: bool = False,
        client: Any | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("GEMINI_API_KEY is required")
        if not allow_paid_models and model_id not in GEMINI_FREE_MODEL_IDS:
            raise PaidModelRejectedError("paid Gemini models are disabled")
        self.model_id = model_id
        self.max_retries = max_retries
        self._client = client or genai.Client(
            api_key=api_key,
            http_options=genai_types.HttpOptions(
                timeout=max(1, int(timeout_seconds * 1000)),
                retry_options=genai_types.HttpRetryOptions(attempts=1),
            ),
        )

    def complete(self, prompt: str, response_model: type[T]) -> LLMResponse[T]:
        validation_schema = response_model.model_json_schema()
        schema = _gemini_input_schema(response_model)
        config = genai_types.GenerateContentConfig(
            system_instruction=(
                "You are FluxGuard's read-only incident investigator. Return only one JSON "
                "object matching the supplied schema. Include every property; use an empty "
                "array only where the schema allows it. Copy evidence IDs exactly from supplied "
                "evidence and never invent them. Do not include application execution metadata, "
                "prose, analysis, or Markdown fences."
            ),
            temperature=0,
            max_output_tokens=GEMINI_MAX_OUTPUT_TOKENS,
            response_mime_type="application/json",
            response_json_schema=schema,
        )
        response = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.models.generate_content(
                    model=self.model_id,
                    contents=prompt,
                    config=config,
                )
                break
            except genai_errors.APIError as exc:
                code = getattr(exc, "code", None)
                if code == 400:
                    raise LLMInvalidRequestError(_gemini_invalid_request_message(exc)) from exc
                if code == 402:
                    raise PaidModelRejectedError("Gemini request requires paid inference") from exc
                if code == 429:
                    if attempt >= self.max_retries:
                        raise LLMRateLimitError(
                            "Gemini rate limit persisted after bounded retries"
                        ) from exc
                    time.sleep(0.5 * (2**attempt))
                    continue
                if code in {408, 504}:
                    if attempt >= self.max_retries:
                        raise LLMTimeoutError("Gemini request timed out") from exc
                    time.sleep(0.5 * (2**attempt))
                    continue
                if isinstance(code, int) and code >= 500 and attempt < self.max_retries:
                    time.sleep(0.5 * (2**attempt))
                    continue
                suffix = f" HTTP {code}" if isinstance(code, int) else ""
                raise LLMError(f"Gemini provider returned{suffix}") from exc
            except (httpx.TimeoutException, TimeoutError) as exc:
                if attempt >= self.max_retries:
                    raise LLMTimeoutError("Gemini request timed out") from exc
                time.sleep(0.5 * (2**attempt))
            except httpx.RequestError as exc:
                if attempt >= self.max_retries:
                    raise LLMError("Gemini provider is unavailable") from exc
                time.sleep(0.5 * (2**attempt))
        if response is None:
            raise LLMError("Gemini provider returned no response")

        candidates = getattr(response, "candidates", None)
        if not isinstance(candidates, list) or not candidates:
            raise LLMInvalidResponseError("Gemini response contained no candidate")
        finish_reason = getattr(candidates[0], "finish_reason", None)
        finish_value = getattr(finish_reason, "value", finish_reason)
        if finish_value == "MAX_TOKENS":
            raise LLMInvalidResponseError("model output was truncated before completion")
        if finish_value not in {None, "STOP", "FINISH_REASON_UNSPECIFIED"}:
            raise LLMInvalidResponseError("Gemini response was blocked before completion")
        try:
            content = response.text
            if not isinstance(content, str) or not content.strip():
                raise LLMInvalidResponseError("Gemini response contained no structured content")
            parsed = _extract_json(content)
            value = response_model.model_validate_json(json.dumps(parsed), strict=True)
        except LLMInvalidResponseError:
            raise
        except ValidationError as exc:
            raise LLMInvalidResponseError(
                _validation_error_message(exc, response_model, validation_schema)
            ) from exc
        except (AttributeError, TypeError, ValueError) as exc:
            raise LLMInvalidResponseError("Gemini response failed schema validation") from exc

        usage = getattr(response, "usage_metadata", None)
        prompt_tokens = getattr(usage, "prompt_token_count", None)
        completion_tokens = getattr(usage, "candidates_token_count", None)
        total_tokens = getattr(usage, "total_token_count", None)
        return LLMResponse(
            value=value,
            model_id=self.model_id,
            usage=LLMUsage(
                prompt_tokens=prompt_tokens if isinstance(prompt_tokens, int) else None,
                completion_tokens=(
                    completion_tokens if isinstance(completion_tokens, int) else None
                ),
                total_tokens=total_tokens if isinstance(total_tokens, int) else None,
                estimated_cost_usd=None,
            ),
        )
