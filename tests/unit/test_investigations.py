import json
from datetime import timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel
from sqlalchemy import func, select

from fluxguard.apps.api.main import create_app
from fluxguard.packages.common.config import Settings
from fluxguard.packages.common.db import get_session
from fluxguard.packages.common.incidents import Detection, record_incident
from fluxguard.packages.common.investigation_agent import InvestigationAgent
from fluxguard.packages.common.investigation_tools import ReadOnlyToolRegistry
from fluxguard.packages.common.investigations import (
    claim_pending_investigation,
    enqueue_investigation,
    execute_investigation,
    recover_interrupted_investigations,
)
from fluxguard.packages.common.llm import (
    LLMInvalidResponseError,
    LLMRateLimitError,
    LLMResponse,
    LLMTimeoutError,
    LLMUsage,
    OpenRouterLLM,
    PaidModelRejectedError,
)
from fluxguard.packages.common.models import (
    IncidentEvidence,
    IncidentSeverity,
    InvestigationRun,
    InvestigationStatus,
)
from fluxguard.packages.common.time import utc_now
from fluxguard.packages.contracts.investigations import (
    ConfidenceAssessment,
    DraftDiagnosis,
    EvidenceHypothesis,
    ToolPlan,
)


class FakeLLM:
    model_id = "fake/free:model:free"

    def __init__(self, responses: list[BaseModel | Exception]) -> None:
        self.responses = responses
        self.calls = 0

    def complete(self, _prompt: str, response_model):
        response = self.responses[self.calls]
        self.calls += 1
        if isinstance(response, Exception):
            raise response
        value = response_model.model_validate(response.model_dump())
        return LLMResponse(
            value=value,
            model_id=self.model_id,
            usage=LLMUsage(10, 5, 15, Decimal("0")),
        )


def create_incident(session_factory) -> tuple[UUID, UUID]:
    with session_factory() as session, session.begin():
        incident, _ = record_incident(
            session,
            Detection(
                incident_type="MALFORMED_EVENT",
                severity=IncidentSeverity.HIGH,
                error_code="MALFORMED_JSON",
                summary="private parser detail",
                source_component="order_consumer",
                fingerprint_scope={"raw_sha256": str(uuid4())},
                evidence_type="QUARANTINED_EVENT",
                evidence_payload={"raw_sha256": "safe", "password": "secret"},
                topic="orders.created.v1",
                partition=0,
                offset=101,
            ),
        )
        evidence_id = session.scalar(
            select(IncidentEvidence.evidence_id).where(
                IncidentEvidence.incident_id == incident.incident_id
            )
        )
        return incident.incident_id, evidence_id


def plan() -> ToolPlan:
    return ToolPlan(
        tools=["get_pipeline_impact", "get_pipeline_topology"],
        competing_hypotheses=["malformed producer payload", "transport corruption"],
        focus="determine where malformed content originated",
    )


def draft(evidence_id: UUID) -> DraftDiagnosis:
    return DraftDiagnosis(
        outcome="COMPLETED",
        likely_root_cause=EvidenceHypothesis(
            claim="The consumer received invalid JSON.",
            rationale="The quarantined record is classified as malformed JSON.",
            supporting_evidence_ids=[evidence_id],
        ),
        alternative_hypotheses=[
            EvidenceHypothesis(
                claim="Transport corruption remains possible.",
                rationale="The evidence does not identify the producing client.",
                supporting_evidence_ids=[evidence_id],
            )
        ],
        confidence=ConfidenceAssessment(
            level="MEDIUM", rationale="One durable record supports it."
        ),
        affected_pipeline_component="order_consumer",
        proposed_next_investigation_steps=["Inspect the producing client logs."],
        uncertainty_missing_information=["Producer identity is unavailable."],
    )


def build_agent(session_factory, fake: FakeLLM, **kwargs) -> InvestigationAgent:
    return InvestigationAgent(
        session_factory=session_factory,
        llm=fake,
        registry=ReadOnlyToolRegistry(evidence_limit=10),
        max_llm_calls=kwargs.get("max_llm_calls", 4),
        max_iterations=kwargs.get("max_iterations", 2),
    )


def test_langgraph_selects_tools_and_returns_grounded_diagnosis(session_factory) -> None:
    incident_id, evidence_id = create_incident(session_factory)
    fake = FakeLLM([plan(), draft(evidence_id)])
    result = build_agent(session_factory, fake).investigate(uuid4(), incident_id)

    assert result.diagnosis.investigation_status == InvestigationStatus.COMPLETED
    assert result.diagnosis.llm_calls == 2
    assert result.diagnosis.total_tokens == 30
    assert {item.tool for item in result.tool_history} >= {
        "get_incident",
        "list_incident_evidence",
        "get_pipeline_impact",
    }
    evidence_call = next(
        item for item in result.tool_history if item.tool == "list_incident_evidence"
    )
    assert evidence_call.output["evidence"][0]["payload"]["password"] == "[REDACTED]"


def test_hallucinated_evidence_gets_one_revision_then_unverified(session_factory) -> None:
    incident_id, evidence_id = create_incident(session_factory)
    invented = uuid4()
    fake = FakeLLM([plan(), draft(invented), draft(invented)])
    result = build_agent(session_factory, fake).investigate(uuid4(), incident_id)

    assert evidence_id != invented
    assert fake.calls == 3
    assert result.diagnosis.investigation_status == InvestigationStatus.UNVERIFIED


def test_insufficient_evidence_is_an_explicit_outcome(session_factory) -> None:
    incident_id, _ = create_incident(session_factory)
    insufficient = DraftDiagnosis(
        outcome="INSUFFICIENT_EVIDENCE",
        likely_root_cause=None,
        alternative_hypotheses=[],
        confidence=ConfidenceAssessment(level="LOW", rationale="No producer identity is present."),
        affected_pipeline_component="order_consumer",
        proposed_next_investigation_steps=["Collect producer logs."],
        uncertainty_missing_information=["Producing client is unknown."],
    )
    result = build_agent(session_factory, FakeLLM([plan(), insufficient])).investigate(
        uuid4(), incident_id
    )
    assert result.diagnosis.investigation_status == InvestigationStatus.INSUFFICIENT_EVIDENCE


def test_persistence_and_call_limit_failure(session_factory) -> None:
    incident_id, _ = create_incident(session_factory)
    with session_factory() as session:
        accepted = enqueue_investigation(session, incident_id, FakeLLM.model_id)
    assert claim_pending_investigation(session_factory) == accepted.investigation_id
    status = execute_investigation(
        session_factory,
        build_agent(session_factory, FakeLLM([plan()]), max_llm_calls=1),
        accepted.investigation_id,
    )
    assert status == InvestigationStatus.FAILED
    with session_factory() as session:
        stored = session.get(InvestigationRun, accepted.investigation_id)
        assert stored.error_category == "MODEL_CALL_LIMIT"
        assert stored.llm_call_count == 1
        assert stored.diagnosis is None


def test_stale_running_investigation_is_marked_interrupted(session_factory) -> None:
    incident_id, _ = create_incident(session_factory)
    with session_factory() as session, session.begin():
        run = InvestigationRun(
            incident_id=incident_id,
            model_id=FakeLLM.model_id,
            status=InvestigationStatus.RUNNING.value,
            started_at=utc_now() - timedelta(hours=1),
            tool_call_history=[],
        )
        session.add(run)
    assert recover_interrupted_investigations(session_factory) == 1
    with session_factory() as session:
        assert session.get(InvestigationRun, run.investigation_id).error_category == (
            "WORKER_INTERRUPTED"
        )


def test_investigation_api_is_development_restricted(session_factory, monkeypatch) -> None:
    incident_id, _ = create_incident(session_factory)

    def sessions():
        with session_factory() as session:
            yield session

    app = create_app(sessions)
    app.dependency_overrides[get_session] = sessions
    client = TestClient(app)
    assert client.get(f"/api/v1/incidents/{incident_id}/investigations").status_code == 404
    monkeypatch.setattr(
        "fluxguard.apps.api.investigations.get_settings",
        lambda: Settings(
            environment="development",
            investigation_api_enabled=True,
        ),
    )
    response = client.post(f"/api/v1/incidents/{incident_id}/investigations")
    assert response.status_code == 202
    investigation_id = response.json()["investigation_id"]
    assert client.get(f"/api/v1/investigations/{investigation_id}").status_code == 200
    assert client.get(f"/api/v1/incidents/{incident_id}/investigations").json()["total"] == 1


def test_paid_model_is_rejected_before_network_access() -> None:
    with pytest.raises(PaidModelRejectedError):
        OpenRouterLLM(
            api_key="not-real",
            model_id="paid/model",
            base_url="https://example.invalid/v1",
            timeout_seconds=1,
            max_retries=0,
        )


@pytest.mark.parametrize(
    ("handler", "expected"),
    [
        (
            lambda _request: (_ for _ in ()).throw(httpx.ReadTimeout("timeout")),
            LLMTimeoutError,
        ),
        (lambda _request: httpx.Response(429), LLMRateLimitError),
        (
            lambda _request: httpx.Response(
                200,
                json={"choices": [{"message": {"content": "not-json"}}]},
            ),
            LLMInvalidResponseError,
        ),
    ],
)
def test_provider_failures_are_categorized(handler, expected) -> None:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    llm = OpenRouterLLM(
        api_key="not-real",
        model_id="test/model:free",
        base_url="https://example.invalid/v1",
        timeout_seconds=1,
        max_retries=0,
        client=client,
    )
    with pytest.raises(expected):
        llm.complete("test", ToolPlan)


def tool_plan_json(**extra) -> dict:
    return {
        "tools": ["list_incident_evidence"],
        "competing_hypotheses": ["producer error", "transport truncation"],
        "focus": "determine the malformed payload source",
        **extra,
    }


def openrouter_response(*, content=None, finish_reason="stop", message_extra=None) -> dict:
    message = {"content": content, **(message_extra or {})}
    return {
        "model": "resolved/free:free",
        "choices": [{"finish_reason": finish_reason, "message": message}],
        "usage": {"prompt_tokens": 12, "completion_tokens": 8, "total_tokens": 20},
    }


def test_fenced_json_is_safely_extracted_from_non_json_model_content() -> None:
    content = "Analysis complete.\n```json\n" + json.dumps(tool_plan_json()) + "\n```"
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=openrouter_response(content=content))
        )
    )
    llm = OpenRouterLLM(
        api_key="not-real",
        model_id="legacy/free:free",
        base_url="https://example.invalid/v1",
        timeout_seconds=1,
        max_retries=0,
        client=client,
    )
    response = llm.complete("test", ToolPlan)
    assert response.value.focus == "determine the malformed payload source"
    assert response.usage.total_tokens == 20


def test_native_json_schema_is_used_only_for_supported_free_model() -> None:
    requests: list[dict] = []

    def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json=openrouter_response(content=json.dumps(tool_plan_json())))

    client = httpx.Client(transport=httpx.MockTransport(handler))
    supported = OpenRouterLLM(
        api_key="not-real",
        model_id="nvidia/nemotron-3.5-super-vl:free",
        base_url="https://example.invalid/v1",
        timeout_seconds=1,
        max_retries=0,
        client=client,
    )
    fallback = OpenRouterLLM(
        api_key="not-real",
        model_id="legacy/free:free",
        base_url="https://example.invalid/v1",
        timeout_seconds=1,
        max_retries=0,
        client=client,
    )
    supported.complete("test", ToolPlan)
    fallback.complete("test", ToolPlan)
    assert requests[0]["response_format"]["type"] == "json_schema"
    assert requests[0]["response_format"]["json_schema"]["strict"] is True
    assert requests[0]["provider"] == {"require_parameters": True}
    assert "response_format" not in requests[1]
    assert "provider" not in requests[1]


def test_tool_call_arguments_are_accepted_as_structured_content() -> None:
    message_extra = {
        "tool_calls": [
            {
                "type": "function",
                "function": {"name": "structured_result", "arguments": tool_plan_json()},
            }
        ]
    }
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json=openrouter_response(
                    content=None, finish_reason="tool_calls", message_extra=message_extra
                ),
            )
        )
    )
    llm = OpenRouterLLM(
        api_key="not-real",
        model_id="legacy/free:free",
        base_url="https://example.invalid/v1",
        timeout_seconds=1,
        max_retries=0,
        client=client,
    )
    assert llm.complete("test", ToolPlan).value.tools == ["list_incident_evidence"]


@pytest.mark.parametrize(
    "payload",
    [
        openrouter_response(
            content=None,
            message_extra={"reasoning": "internal reasoning without a final answer"},
        ),
        openrouter_response(content='{"tools":', finish_reason="length"),
        openrouter_response(
            content="```json\n{}\n```\n```json\n{}\n```",
        ),
        openrouter_response(content=json.dumps(tool_plan_json(unexpected="rejected"))),
    ],
)
def test_reasoning_truncation_ambiguous_blocks_and_extra_fields_are_rejected(payload) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=payload))
    )
    llm = OpenRouterLLM(
        api_key="not-real",
        model_id="legacy/free:free",
        base_url="https://example.invalid/v1",
        timeout_seconds=1,
        max_retries=0,
        client=client,
    )
    with pytest.raises(LLMInvalidResponseError):
        llm.complete("test", ToolPlan)


def test_persisted_history_contains_no_api_key(session_factory) -> None:
    incident_id, evidence_id = create_incident(session_factory)
    with session_factory() as session:
        accepted = enqueue_investigation(session, incident_id, FakeLLM.model_id)
    claim_pending_investigation(session_factory)
    execute_investigation(
        session_factory,
        build_agent(session_factory, FakeLLM([plan(), draft(evidence_id)])),
        accepted.investigation_id,
    )
    with session_factory() as session:
        run = session.get(InvestigationRun, accepted.investigation_id)
        serialized = str(run.tool_call_history) + str(run.diagnosis)
        assert "not-real" not in serialized
        assert session.scalar(select(func.count()).select_from(InvestigationRun)) == 1
