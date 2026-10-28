import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, TypedDict
from uuid import UUID

from langgraph.graph import END, START, StateGraph
from sqlalchemy.orm import Session, sessionmaker

from fluxguard.packages.common.investigation_tools import ReadOnlyToolRegistry
from fluxguard.packages.common.llm import InvestigationLLM, LLMCallLimitError, LLMResponse
from fluxguard.packages.common.models import InvestigationStatus
from fluxguard.packages.contracts.investigations import (
    Diagnosis,
    DraftDiagnosis,
    EvidenceToolOutput,
    RelatedIncidentsToolOutput,
    ToolCallRecord,
    ToolPlan,
)

MAX_MODEL_CONTEXT_CHARS = 60_000


class InvestigationState(TypedDict, total=False):
    investigation_id: UUID
    incident_id: UUID
    tool_plan: ToolPlan
    tool_outputs: dict[str, dict[str, Any]]
    tool_history: list[ToolCallRecord]
    draft: DraftDiagnosis
    diagnosis: Diagnosis
    grounding_errors: list[str]
    grounding_ok: bool
    iteration: int
    revision_count: int
    llm_calls: int
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    estimated_cost_usd: Decimal
    actual_model_id: str
    started_at_monotonic: float


@dataclass(frozen=True)
class InvestigationResult:
    diagnosis: Diagnosis
    tool_history: list[ToolCallRecord]


def _bounded_context(outputs: dict[str, dict[str, Any]]) -> str:
    rendered = json.dumps(outputs, sort_keys=True, default=str)
    if len(rendered) <= MAX_MODEL_CONTEXT_CHARS:
        return rendered
    return rendered[:MAX_MODEL_CONTEXT_CHARS] + "\n[CONTEXT TRUNCATED AT BOUNDED LIMIT]"


class InvestigationAgent:
    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        llm: InvestigationLLM,
        registry: ReadOnlyToolRegistry,
        max_llm_calls: int = 4,
        max_iterations: int = 2,
    ) -> None:
        self.session_factory = session_factory
        self.llm = llm
        self.registry = registry
        self.max_llm_calls = max_llm_calls
        self.max_iterations = max_iterations
        self.last_run_metrics: dict[str, Any] = {}
        self.graph = self._build_graph()

    def _invoke_llm(
        self,
        state: InvestigationState,
        prompt: str,
        response_model: type[ToolPlan | DraftDiagnosis],
    ) -> tuple[ToolPlan | DraftDiagnosis, dict[str, Any]]:
        calls = state.get("llm_calls", 0)
        if calls >= self.max_llm_calls:
            raise LLMCallLimitError("per-investigation model call limit reached")
        self.last_run_metrics["llm_calls"] = self.last_run_metrics.get("llm_calls", 0) + 1
        response: LLMResponse = self.llm.complete(prompt, response_model)
        usage = response.usage
        cost = state.get("estimated_cost_usd", Decimal("0"))
        if usage.estimated_cost_usd is not None:
            cost += usage.estimated_cost_usd
        counters = {
            "llm_calls": calls + 1,
            "prompt_tokens": state.get("prompt_tokens", 0) + (usage.prompt_tokens or 0),
            "completion_tokens": state.get("completion_tokens", 0) + (usage.completion_tokens or 0),
            "total_tokens": state.get("total_tokens", 0) + (usage.total_tokens or 0),
            "estimated_cost_usd": cost,
            "actual_model_id": response.model_id,
        }
        self.last_run_metrics.update(counters)
        return response.value, counters

    def _load_incident(self, state: InvestigationState) -> dict[str, Any]:
        with self.session_factory() as session:
            output, record = self.registry.execute(session, "get_incident", state["incident_id"])
        return {
            "tool_outputs": {"get_incident": output.model_dump(mode="json")},
            "tool_history": [record],
        }

    def _select_evidence(self, state: InvestigationState) -> dict[str, Any]:
        prompt = (
            "Select approved read-only tools for this incident. Consider at least two competing "
            "hypotheses before diagnosis. Approved tools: "
            f"{', '.join(self.registry.names)}. Incident: "
            f"{_bounded_context(state['tool_outputs'])}"
        )
        plan, counters = self._invoke_llm(state, prompt, ToolPlan)
        assert isinstance(plan, ToolPlan)
        tools = list(dict.fromkeys(["list_incident_evidence", *plan.tools]))
        plan = plan.model_copy(update={"tools": tools[:6]})
        return {"tool_plan": plan, **counters}

    def _execute_tools(self, state: InvestigationState) -> dict[str, Any]:
        outputs = dict(state.get("tool_outputs", {}))
        history = list(state.get("tool_history", []))
        with self.session_factory() as session:
            for name in state["tool_plan"].tools:
                if name in outputs:
                    continue
                output, record = self.registry.execute(session, name, state["incident_id"])
                outputs[name] = output.model_dump(mode="json")
                history.append(record)
        return {"tool_outputs": outputs, "tool_history": history}

    def _analyze_hypotheses(self, state: InvestigationState) -> dict[str, Any]:
        revision = state.get("revision_count", 0)
        revision_note = ""
        if revision:
            revision_note = (
                "\nThe previous draft failed independent grounding for these reasons: "
                + "; ".join(state.get("grounding_errors", []))
                + ". Revise once using only genuine evidence IDs in the supplied context."
            )
        prompt = (
            "Analyze the competing hypotheses below using only tool evidence. Every causal claim "
            "must cite supporting evidence IDs copied exactly from the tool evidence. For "
            "COMPLETED, provide a supported likely_root_cause. Use INSUFFICIENT_EVIDENCE when "
            "facts cannot support a diagnosis, set likely_root_cause to null, and do not create "
            "unsupported alternative hypotheses. Confidence is an uncalibrated assessment, "
            "never a probability.\n"
            f"Hypotheses: {json.dumps(state['tool_plan'].competing_hypotheses)}\n"
            f"Tool evidence: {_bounded_context(state['tool_outputs'])}{revision_note}"
        )
        draft, counters = self._invoke_llm(state, prompt, DraftDiagnosis)
        assert isinstance(draft, DraftDiagnosis)
        return {"draft": draft, **counters}

    def _check_evidence_support(self, state: InvestigationState) -> dict[str, Any]:
        outputs = state["tool_outputs"]
        valid: set[UUID] = set()
        evidence_payload = outputs.get("list_incident_evidence")
        if evidence_payload:
            evidence = EvidenceToolOutput.model_validate(evidence_payload)
            valid.update(item.evidence_id for item in evidence.evidence)
        related_payload = outputs.get("get_related_incidents")
        if related_payload:
            related = RelatedIncidentsToolOutput.model_validate(related_payload)
            valid.update(
                evidence_id for context in related.incidents for evidence_id in context.evidence_ids
            )
        errors: list[str] = []
        draft = state["draft"]
        if draft.outcome == "COMPLETED" and draft.likely_root_cause is None:
            errors.append("completed diagnosis omitted a likely root cause")
        hypotheses = [
            item
            for item in [draft.likely_root_cause, *draft.alternative_hypotheses]
            if item is not None
        ]
        for hypothesis in hypotheses:
            referenced = {
                *hypothesis.supporting_evidence_ids,
                *hypothesis.contradicting_evidence_ids,
            }
            invented = sorted(referenced - valid, key=str)
            if invented:
                errors.append(
                    f"claim references unavailable evidence: {', '.join(map(str, invented))}"
                )
            if not hypothesis.supporting_evidence_ids:
                errors.append("claim has no supporting evidence")
        return {"grounding_ok": not errors, "grounding_errors": errors}

    def _route_after_grounding(self, state: InvestigationState) -> str:
        if state["grounding_ok"]:
            return "FINALIZE"
        if state.get("revision_count", 0) < 1 and state.get("iteration", 1) < self.max_iterations:
            return "GATHER_MORE_EVIDENCE"
        return "FINALIZE"

    def _gather_more_evidence(self, state: InvestigationState) -> dict[str, Any]:
        existing = set(state["tool_outputs"])
        additions = [
            name
            for name in (
                "get_pipeline_impact",
                "get_event_processing_status",
                "get_related_incidents",
                "get_pipeline_topology",
            )
            if name not in existing
        ]
        plan = state["tool_plan"].model_copy(
            update={"tools": additions or list(state["tool_plan"].tools)}
        )
        return {
            "tool_plan": plan,
            "revision_count": state.get("revision_count", 0) + 1,
            "iteration": state.get("iteration", 1) + 1,
        }

    def _finalize(self, state: InvestigationState) -> dict[str, Any]:
        draft = state["draft"]
        if not state["grounding_ok"]:
            status = InvestigationStatus.UNVERIFIED
        elif draft.outcome == "INSUFFICIENT_EVIDENCE":
            status = InvestigationStatus.INSUFFICIENT_EVIDENCE
        else:
            status = InvestigationStatus.COMPLETED
        diagnosis = Diagnosis(
            **draft.model_dump(),
            investigation_id=state["investigation_id"],
            incident_id=state["incident_id"],
            investigation_status=status,
            model_id=state.get("actual_model_id", self.llm.model_id),
            llm_calls=state.get("llm_calls", 0),
            prompt_tokens=state.get("prompt_tokens") or None,
            completion_tokens=state.get("completion_tokens") or None,
            total_tokens=state.get("total_tokens") or None,
            estimated_cost_usd=state.get("estimated_cost_usd"),
            elapsed_seconds=max(0.0, time.monotonic() - state["started_at_monotonic"]),
        )
        return {"diagnosis": diagnosis}

    def _build_graph(self):
        graph = StateGraph(InvestigationState)
        graph.add_node("LOAD_INCIDENT", self._load_incident)
        graph.add_node("SELECT_EVIDENCE_TO_INSPECT", self._select_evidence)
        graph.add_node("EXECUTE_READ_ONLY_TOOLS", self._execute_tools)
        graph.add_node("ANALYZE_HYPOTHESES", self._analyze_hypotheses)
        graph.add_node("CHECK_EVIDENCE_SUPPORT", self._check_evidence_support)
        graph.add_node("GATHER_MORE_EVIDENCE", self._gather_more_evidence)
        graph.add_node("FINALIZE", self._finalize)
        graph.add_edge(START, "LOAD_INCIDENT")
        graph.add_edge("LOAD_INCIDENT", "SELECT_EVIDENCE_TO_INSPECT")
        graph.add_edge("SELECT_EVIDENCE_TO_INSPECT", "EXECUTE_READ_ONLY_TOOLS")
        graph.add_edge("EXECUTE_READ_ONLY_TOOLS", "ANALYZE_HYPOTHESES")
        graph.add_edge("ANALYZE_HYPOTHESES", "CHECK_EVIDENCE_SUPPORT")
        graph.add_conditional_edges(
            "CHECK_EVIDENCE_SUPPORT",
            self._route_after_grounding,
            {
                "GATHER_MORE_EVIDENCE": "GATHER_MORE_EVIDENCE",
                "FINALIZE": "FINALIZE",
            },
        )
        graph.add_edge("GATHER_MORE_EVIDENCE", "EXECUTE_READ_ONLY_TOOLS")
        graph.add_edge("FINALIZE", END)
        return graph.compile()

    def investigate(self, investigation_id: UUID, incident_id: UUID) -> InvestigationResult:
        self.last_run_metrics = {
            "llm_calls": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "estimated_cost_usd": Decimal("0"),
            "actual_model_id": self.llm.model_id,
        }
        final = self.graph.invoke(
            {
                "investigation_id": investigation_id,
                "incident_id": incident_id,
                "tool_outputs": {},
                "tool_history": [],
                "iteration": 1,
                "revision_count": 0,
                "llm_calls": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "estimated_cost_usd": Decimal("0"),
                "started_at_monotonic": time.monotonic(),
            },
            config={"recursion_limit": 20},
        )
        return InvestigationResult(diagnosis=final["diagnosis"], tool_history=final["tool_history"])
