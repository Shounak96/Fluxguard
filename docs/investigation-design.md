# Evidence-grounded investigation design

## State machine

```text
START -> LOAD_INCIDENT -> SELECT_EVIDENCE_TO_INSPECT -> EXECUTE_READ_ONLY_TOOLS
      -> ANALYZE_HYPOTHESES -> CHECK_EVIDENCE_SUPPORT
      -> GATHER_MORE_EVIDENCE -> EXECUTE_READ_ONLY_TOOLS (at most once)
      -> FINALIZE -> END
```

The model selects evidence tools and analyzes at least two competing hypotheses. Deterministic
nodes load the incident, execute tools, validate citations, and finalize status. Iterations and LLM
calls have independent hard limits. Grounding failure permits one revision; another failure is
stored as `UNVERIFIED`. Lack of support is a valid `INSUFFICIENT_EVIDENCE` outcome.

## Tool contracts

The closed registry contains `get_incident`, `list_incident_evidence`, `get_pipeline_impact`,
`get_event_processing_status`, `get_related_incidents`, and `get_pipeline_topology`. Application
code constructs typed inputs with bounded pagination. Tools reuse SQLAlchemy models and existing
incident services and have no mutation, arbitrary SQL, shell, or network capability.

## Grounding and output

Every causal hypothesis requires supporting evidence IDs. An independent validator accepts IDs
only when they belong to the investigated incident or related context returned by an approved
tool. Model assertions never validate themselves. Diagnoses include alternatives, contradicting
evidence, missing information, next steps, component, model/call/token metadata, latency, and an
explicit notice that confidence is not a calibrated probability.

## Provider and token boundaries

`InvestigationLLM` is provider-independent; `OpenRouterLLM` and `GeminiLLM` are selected with
`LLM_PROVIDER=openrouter|gemini`. Requests use timeouts, exponential bounded retries, rate-limit
handling, native structured output where supported, strict local validation, and a per-run call
cap. Prompt context is capped at 60,000 characters. Output is bounded at 4,096 tokens for the
configured Nemotron and Gemini models (1,800 for other OpenRouter models). OpenRouter model IDs
must end in `:free`, and Gemini models must be in the explicit free-model allowlist unless paid
models are deliberately enabled. Automatic provider/model fallback is never attempted. Unknown
pricing is reported as unavailable; configured OpenRouter free models record zero estimated cost.

Set only the selected provider key in the ignored `.env` or process environment:

```powershell
$env:INVESTIGATION_API_ENABLED = "true"
# Set LLM_PROVIDER and its matching OPENROUTER_API_KEY or GEMINI_API_KEY in .env.
# Do not commit or print provider credentials.
docker compose up --build -d --wait
```

## Persistence and failure handling

Migration `0003_ai_investigations` stores state, sanitized diagnosis, bounded tool history, token and
cost metadata, timestamps, and a categorized error. Raw prompts, provider responses, and API keys
are not stored. API execution is asynchronous: POST creates `PENDING`, and the worker claims it
with a row lock. Stale `RUNNING` rows become `FAILED/WORKER_INTERRUPTED` after restart.

## Security boundaries and limitations

The API gate is suitable only for local development and is not authentication. The model cannot
change incidents or pipeline data and cannot perform recovery. Tool history can contain sanitized
operational metadata. Metrics are process-local, investigation workers do not yet expose a health
probe, and model availability/rate limits remain external dependencies.
The investigator currently shares the application database role; least privilege is enforced by
the closed tool layer rather than a dedicated PostgreSQL read-only role in this local milestone.
The exploratory live benchmark completed 1 of 10 investigations. Its one validated diagnosis used
verified evidence and appropriate uncertainty, but provided no evaluable definitive root cause.
This small historical sample does not establish provider reliability or root-cause accuracy.

## Exploratory diagnostic evaluation

`fluxguard-evaluate-investigations --json-output report.json` evaluates persisted investigations
without invoking an LLM. Reports label the historical dataset as exploratory and not statistically
representative. Live-provider runs and deterministic fake-model fixtures are separate cohorts.
The Milestone 3 snapshot is stored in `docs/milestone-3-benchmark-report.json` and can be regenerated
with the same command against the retained PostgreSQL investigation records.

Operational completion is `validated diagnoses / investigation attempts`. Diagnostic-quality
rates are computed only from validated diagnoses for which that dimension is available; each rate
includes its numerator, denominator, and unavailable count. Provider or transport failures are
reported operationally, including sanitized HTTP status counts, and never become incorrect
diagnoses. A justified `INSUFFICIENT_EVIDENCE` result can leave root-cause correctness unavailable
while still receiving credit for fault recognition, evidence grounding, and appropriate uncertainty.
