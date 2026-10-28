import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from google.genai import errors as genai_errors

from fluxguard.apps.worker.investigator import build_llm
from fluxguard.packages.common.config import Settings
from fluxguard.packages.common.llm import (
    GeminiLLM,
    LLMInvalidRequestError,
    LLMInvalidResponseError,
    LLMRateLimitError,
    LLMTimeoutError,
    OpenRouterLLM,
    PaidModelRejectedError,
)
from fluxguard.packages.contracts.investigations import (
    CONFIDENCE_CALIBRATION_NOTICE,
    ConfidenceAssessment,
    DraftDiagnosis,
    EvidenceHypothesis,
)


def valid_draft() -> DraftDiagnosis:
    return DraftDiagnosis(
        outcome="COMPLETED",
        likely_root_cause=EvidenceHypothesis(
            claim="The consumer received invalid JSON.",
            rationale="Durable evidence classifies the record as malformed JSON.",
            supporting_evidence_ids=[uuid4()],
        ),
        alternative_hypotheses=[],
        confidence=ConfidenceAssessment(level="MEDIUM", rationale="One record supports it."),
        affected_pipeline_component="order_consumer",
        proposed_next_investigation_steps=["Inspect producer logs."],
        uncertainty_missing_information=["Producer identity is unavailable."],
    )


def openrouter_response(content: str) -> dict:
    return {
        "model": "resolved/free:free",
        "choices": [{"finish_reason": "stop", "message": {"content": content}}],
        "usage": {"prompt_tokens": 12, "completion_tokens": 8, "total_tokens": 20},
    }


class FakeGeminiModels:
    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[dict] = []

    def generate_content(self, **kwargs):
        self.calls.append(kwargs)
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class FakeGeminiClient:
    def __init__(self, results) -> None:
        self.models = FakeGeminiModels(results)


def gemini_response(content: str, finish_reason: str = "STOP"):
    return SimpleNamespace(
        text=content,
        candidates=[SimpleNamespace(finish_reason=finish_reason)],
        usage_metadata=SimpleNamespace(
            prompt_token_count=21,
            candidates_token_count=13,
            total_token_count=34,
        ),
    )


def test_model_schema_excludes_application_owned_fields_and_requires_arrays() -> None:
    requests: list[dict] = []

    def handler(request):
        requests.append(json.loads(request.content))
        content = valid_draft().model_dump(mode="json")
        content["confidence"].pop("calibration_notice")
        return httpx.Response(200, json=openrouter_response(json.dumps(content)))

    llm = OpenRouterLLM(
        api_key="not-real",
        model_id="nvidia/nemotron-3.5-super-vl:free",
        base_url="https://example.invalid/v1",
        timeout_seconds=1,
        max_retries=0,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    result = llm.complete("test", DraftDiagnosis)
    schema = requests[0]["response_format"]["json_schema"]["schema"]
    confidence_schema = schema["$defs"]["ConfidenceAssessment"]
    hypothesis_schema = schema["$defs"]["EvidenceHypothesis"]

    assert "calibration_notice" not in confidence_schema["properties"]
    assert "alternative_hypotheses" in schema["required"]
    assert "contradicting_evidence_ids" in hypothesis_schema["required"]
    assert result.value.confidence.calibration_notice == CONFIDENCE_CALIBRATION_NOTICE
    assert requests[0]["max_tokens"] == 4096
    assert requests[0]["reasoning_effort"] == "none"
    assert "reasoning" not in requests[0]
    assert "include_reasoning" not in requests[0]


def test_other_models_keep_compatible_bounded_request_defaults() -> None:
    requests: list[dict] = []

    def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json=openrouter_response(json.dumps(valid_draft().model_dump(mode="json"))),
        )

    llm = OpenRouterLLM(
        api_key="not-real",
        model_id="legacy/free:free",
        base_url="https://example.invalid/v1",
        timeout_seconds=1,
        max_retries=0,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    llm.complete("test", DraftDiagnosis)

    assert requests[0]["max_tokens"] == 1800
    assert "reasoning_effort" not in requests[0]
    assert "reasoning" not in requests[0]
    assert "include_reasoning" not in requests[0]
    assert "response_format" not in requests[0]


def test_truncated_structured_response_still_fails_safely() -> None:
    payload = {
        "choices": [
            {
                "finish_reason": "length",
                "message": {"content": '{"outcome":"COMPLETED"'},
            }
        ]
    }
    llm = OpenRouterLLM(
        api_key="not-real",
        model_id="nvidia/nemotron-3.5-super-vl:free",
        base_url="https://example.invalid/v1",
        timeout_seconds=1,
        max_retries=0,
        client=httpx.Client(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=payload))
        ),
    )

    with pytest.raises(LLMInvalidResponseError, match="truncated before completion"):
        llm.complete("test", DraftDiagnosis)


def test_validation_diagnostics_include_only_safe_fields_and_error_types() -> None:
    unsafe_value = "sk-sensitive-model-content"
    content = valid_draft().model_dump(mode="json")
    content["confidence"] = {
        "level": "VERY_HIGH",
        unsafe_value: unsafe_value,
    }
    llm = OpenRouterLLM(
        api_key="not-real",
        model_id="legacy/free:free",
        base_url="https://example.invalid/v1",
        timeout_seconds=1,
        max_retries=0,
        client=httpx.Client(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, json=openrouter_response(json.dumps(content)))
            )
        ),
    )

    with pytest.raises(LLMInvalidResponseError) as captured:
        llm.complete("test", DraftDiagnosis)

    message = str(captured.value)
    assert "confidence.level=literal_error" in message
    assert "confidence.rationale=missing" in message
    assert "confidence.<unknown-field>=extra_forbidden" in message
    assert unsafe_value not in message


def test_gemini_uses_native_schema_and_strict_application_validation() -> None:
    content = valid_draft().model_dump(mode="json")
    content["confidence"].pop("calibration_notice")
    client = FakeGeminiClient([gemini_response(json.dumps(content))])
    llm = GeminiLLM(
        api_key="not-real",
        model_id="gemini-3.5-flash-lite",
        timeout_seconds=30,
        max_retries=0,
        client=client,
    )

    result = llm.complete("test", DraftDiagnosis)
    config = client.models.calls[0]["config"]
    schema = config.response_json_schema

    assert config.response_mime_type == "application/json"
    assert config.max_output_tokens == 4096
    rendered_schema = json.dumps(schema)
    for omitted in (
        "additionalProperties",
        "format",
        "maxItems",
        "maxLength",
        "minItems",
        "minLength",
        "title",
    ):
        assert f'"{omitted}"' not in rendered_schema
    assert "$defs" in schema
    assert "$ref" in rendered_schema
    assert "anyOf" in rendered_schema
    assert result.value.outcome == "COMPLETED"
    assert result.usage.total_tokens == 34
    assert result.usage.estimated_cost_usd is None


def test_gemini_invalid_schema_response_is_safely_rejected() -> None:
    unsafe_value = "private-model-output"
    content = valid_draft().model_dump(mode="json")
    content["confidence"] = {"level": "IMPOSSIBLE", unsafe_value: unsafe_value}
    llm = GeminiLLM(
        api_key="not-real",
        model_id="gemini-3.5-flash-lite",
        timeout_seconds=30,
        max_retries=0,
        client=FakeGeminiClient([gemini_response(json.dumps(content))]),
    )

    with pytest.raises(LLMInvalidResponseError) as captured:
        llm.complete("test", DraftDiagnosis)

    assert "confidence.level=literal_error" in str(captured.value)
    assert unsafe_value not in str(captured.value)


def test_gemini_missing_key_and_paid_model_are_rejected_without_network() -> None:
    with pytest.raises(ValueError, match="GEMINI_API_KEY"):
        GeminiLLM(
            api_key="",
            model_id="gemini-3.5-flash-lite",
            timeout_seconds=30,
            max_retries=0,
        )
    with pytest.raises(PaidModelRejectedError):
        GeminiLLM(
            api_key="not-real",
            model_id="gemini-2.5-pro",
            timeout_seconds=30,
            max_retries=0,
            client=FakeGeminiClient([]),
        )


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (httpx.ReadTimeout("timeout"), LLMTimeoutError),
        (
            genai_errors.APIError(429, {"error": {"code": 429, "message": "limited"}}),
            LLMRateLimitError,
        ),
    ],
)
def test_gemini_timeout_and_rate_limit_are_categorized(error, expected) -> None:
    llm = GeminiLLM(
        api_key="not-real",
        model_id="gemini-3.5-flash-lite",
        timeout_seconds=30,
        max_retries=0,
        client=FakeGeminiClient([error]),
    )
    with pytest.raises(expected):
        llm.complete("test", DraftDiagnosis)


def test_gemini_http_400_retains_only_safe_classification_and_paths() -> None:
    unsafe_description = "invalid value copied from private incident payload"
    error = genai_errors.APIError(
        400,
        {
            "error": {
                "code": 400,
                "status": "INVALID_ARGUMENT",
                "message": "Response schema has too many states: " + unsafe_description,
                "details": [
                    {
                        "fieldViolations": [
                            {
                                "field": (
                                    "generation_config.response_schema.properties.likely_root_cause"
                                ),
                                "description": unsafe_description,
                            }
                        ]
                    }
                ],
            }
        },
    )
    llm = GeminiLLM(
        api_key="not-real",
        model_id="gemini-3.5-flash-lite",
        timeout_seconds=30,
        max_retries=0,
        client=FakeGeminiClient([error]),
    )

    with pytest.raises(LLMInvalidRequestError) as captured:
        llm.complete("test", DraftDiagnosis)

    message = str(captured.value)
    assert "HTTP 400" in message
    assert "status=INVALID_ARGUMENT" in message
    assert "reason=schema_complexity" in message
    assert "generation_config.response_schema.properties.likely_root_cause" in message
    assert unsafe_description not in message


def test_gemini_retries_are_bounded_and_sdk_retries_are_disabled(monkeypatch) -> None:
    captured: dict = {}
    fake_client = FakeGeminiClient([])

    def client_factory(**kwargs):
        captured.update(kwargs)
        return fake_client

    monkeypatch.setattr("fluxguard.packages.common.llm.genai.Client", client_factory)
    GeminiLLM(
        api_key="not-real",
        model_id="gemini-3.5-flash-lite",
        timeout_seconds=12.5,
        max_retries=2,
    )

    options = captured["http_options"]
    assert options.timeout == 12_500
    assert options.retry_options.attempts == 1

    client = FakeGeminiClient(
        [
            genai_errors.APIError(429, {"error": {"code": 429, "message": "limited"}}),
            gemini_response(json.dumps(valid_draft().model_dump(mode="json"))),
        ]
    )
    monkeypatch.setattr("fluxguard.packages.common.llm.time.sleep", lambda _delay: None)
    GeminiLLM(
        api_key="not-real",
        model_id="gemini-3.5-flash-lite",
        timeout_seconds=30,
        max_retries=1,
        client=client,
    ).complete("test", DraftDiagnosis)
    assert len(client.models.calls) == 2


def test_configured_provider_selection_preserves_openrouter(monkeypatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    gemini_client = FakeGeminiClient([])
    gemini = build_llm(
        Settings(
            _env_file=None,
            llm_provider="gemini",
            gemini_api_key="not-real",
        ),
        client=gemini_client,
    )
    openrouter = build_llm(
        Settings(
            _env_file=None,
            llm_provider="openrouter",
            openrouter_api_key="not-real",
        ),
        client=httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(500))),
    )

    assert isinstance(gemini, GeminiLLM)
    assert gemini.model_id == "gemini-3.5-flash-lite"
    assert Settings(_env_file=None, llm_provider="gemini").active_investigation_model_is_free
    assert not Settings(
        _env_file=None,
        llm_provider="gemini",
        gemini_model_id="gemini-2.5-flash-lite",
    ).active_investigation_model_is_free
    assert isinstance(openrouter, OpenRouterLLM)

    with pytest.raises(RuntimeError, match="GEMINI_API_KEY"):
        build_llm(Settings(_env_file=None, llm_provider="gemini"), client=gemini_client)
