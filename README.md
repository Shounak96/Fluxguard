# FluxGuard

**Evidence-grounded investigation and invariant-verified recovery for distributed event pipelines.**

FluxGuard is a local-first engineering project for detecting, investigating, and safely recovering
failed events without turning replay into an unrestricted mutation mechanism. It combines an
at-least-once order pipeline with durable incident evidence, a bounded AI investigation layer, and
deterministic PostgreSQL-backed recovery controls.

Production approval and recovery execution are intentionally disabled. The implemented recovery
path is available only through trusted internal code and isolated test/demo fixtures.

## The problem

Distributed event pipelines fail in ambiguous ways: publishers time out after acknowledgement,
consumers receive malformed or unsupported events, retries exhaust, and projections conflict with
authoritative state. Blind replay is dangerous because it can duplicate business effects, overwrite
healthy records, or apply data that never satisfied the original business invariants.

## The approach

FluxGuard separates diagnosis from authority:

1. The pipeline persists exact failure evidence and correlates it to an incident.
2. A bounded, read-only investigator can propose evidence-cited hypotheses and uncertainty.
3. Deterministic policy decides whether a closed recovery action is eligible.
4. A PostgreSQL dry run simulates the declared write set and always rolls back.
5. Approval binds the exact plan, evidence, state digest, policy, verifier version, and expiry.
6. The internal executor reloads every precondition and commits the business effect with immutable
   audit records—or fails closed.

An AI diagnosis can provide context, but it cannot approve a plan, execute recovery, issue arbitrary
SQL, or bypass deterministic evidence and invariant checks.

## Architecture

| Area | Implemented responsibility |
| --- | --- |
| FastAPI | Validates idempotent order commands, calculates exact `Decimal` totals, and atomically writes orders plus outbox records. |
| PostgreSQL | Authoritative orders, transactional outbox, projections, processed-event markers, quarantine, incidents, evidence, investigations, recovery plans, approvals, execution records, and audit history. |
| Outbox publisher | Locks pending rows, publishes with `acks=all`, and records broker acknowledgement. |
| Redpanda | Kafka-compatible local event transport for versioned order events. |
| Consumer | Validates contracts and invariants, applies projections idempotently, quarantines permanent failures, and never advances offsets past unpersisted failures. |
| Incident detection | Classifies failures deterministically and persists bounded, redacted, correlated evidence. |
| Investigator | Runs a bounded LangGraph workflow using an explicitly selected OpenRouter or Gemini adapter and six approved read-only tools. |
| Recovery policy | Produces typed, explainable eligibility decisions for a closed action catalog; missing or uncertain evidence fails closed. |
| Verifier | Reloads authoritative state, simulates the declared action in a dedicated PostgreSQL transaction, checks invariants/write sets, and unconditionally rolls back. |
| Approval and executor | Require trusted authorization and fresh persisted bindings; execution is idempotent, concurrency-controlled, internal-only, and audited. |
| Benchmark harness | Creates owned disposable PostgreSQL/Redpanda resources, records evidence-classed outcomes and timings, and emits independently verifiable artifacts. |

The supported recovery actions are `REPLAY_VERIFIED_EVENT`, `RESCHEDULE_OUTBOX_EVENT`,
`RETAIN_QUARANTINE`, and `REQUEST_MANUAL_REVIEW`. Recovery never synthesizes missing event bytes,
publishes directly to Kafka, or exposes an arbitrary SQL interface.

See [architecture](docs/architecture.md), [engineering decisions](docs/decisions.md), and the
[recovery design](docs/milestone-4-recovery-design.md) for the deeper system boundaries. The
[component, trust-boundary, and recovery-sequence diagrams](docs/architecture-diagrams.md) provide
a compact visual overview.

## Technical highlights

- At-least-once publication with idempotent business effects
- Transactional outbox and manual Kafka offset management
- Versioned Pydantic contracts and exact financial invariants
- Durable quarantine, incident correlation, and persisted failure evidence
- Provider-independent, bounded AI investigation with competing hypotheses
- Strict structured-output validation and independent evidence-citation verification
- Deterministic recovery policy with stable plan identities and state/evidence digests
- PostgreSQL `REPEATABLE READ` rollback-only verification
- Declared write-set, postcondition, and unrelated-state validation
- Approval-bound, expiring, idempotent internal execution
- Immutable lifecycle, verification, approval, execution, and failure audit records
- Bounded concurrency validation against consumer, publisher, and executor races
- Reproducible benchmark isolation and checksummed JSON artifact bundles

FluxGuard deliberately does not claim end-to-end exactly-once delivery. A publisher can crash after
the broker acknowledges a record but before PostgreSQL records that acknowledgement; downstream
idempotency prevents a second business effect.

## Verified benchmark evidence

### Recovery-safety benchmark

The bounded local benchmark used disposable PostgreSQL data and run-scoped Redpanda resources. It
recorded 19 `REAL_INTEGRATION` trials and one separately classified
`DETERMINISTIC_SIMULATION`—the cohorts are not pooled into a single success rate.

| Measurement | Result | Denominator |
| --- | ---: | --- |
| Eligible recovery success | 8/8 | Real trials whose expected outcome was `RECOVERED` |
| Unsafe-action rejection | 7/7 | Real trials whose expected outcome was `SAFELY_REJECTED` |
| Concurrency safety | 9/9 | Three trials each for executor/executor, consumer/executor, and publisher/executor races |
| Duplicate business effects | 0/19 real; 0/1 simulation | Every conclusive result in its evidence cohort |
| Invariant violations | 0/19 real; 0/1 simulation | Every conclusive result in its evidence cohort |

The remaining real outcomes were one verified no-op and three expected operational conflicts in the
consumer/executor race. The simulation injected a transactional failure and verified rollback; it
was not a real process crash.

### GitHub-hosted end-to-end demo

The manually dispatched GitHub Actions demo produced a schema-v2 benchmark report, schema-v1
operational summary, and checksum manifest:

| Measurement | Result | Denominator |
| --- | ---: | --- |
| Recovery | 1/1 | Recoverable outbox-failure lane |
| Safe rejection | 1/1 | Unsupported-schema lane |
| Audit consistency | 2/2 | Both persisted incident/recovery traces |
| Duplicate business effects | 0/2 | Both real-integration lanes |
| Invariant violations | 0/2 | Both real-integration lanes |
| Recoverable path latency | 4,779.858 ms | One hosted run |
| Recovery execution latency | 39.274 ms | One hosted execution sample |

The publication fault is an injected failing publisher, not an actual broker outage. Approval uses
an isolated test identity, deterministic evidence inspection replaces live AI investigation, and
the sample is a reproducibility result—not a throughput result, availability claim, or production
service-level objective.

## Reproduce the Demo

The [two-minute demo walkthrough](docs/demo-walkthrough.md) follows the real transactional-outbox,
incident, verification, approval-bound recovery, unsafe-rejection, and audit paths. It includes the
exact local command, manual GitHub Actions steps, artifact verification commands, observed hosted
results, and limitations; the hosted demo performs no live LLM investigation.

## Run locally

### Prerequisites

- Python 3.12+
- Docker Desktop with Compose v2
- PowerShell 7 recommended on Windows

Runtime and development dependencies are pinned in `pyproject.toml`; container images are pinned in
`compose.yaml`. No paid service or model is required for the pipeline, tests, recovery benchmarks,
or demo.

### Install and start

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up --build -d --wait
docker compose ps
Invoke-RestMethod http://localhost:8000/health/ready
```

The API applies Alembic migrations on startup. Values in `.env.example` are local-only defaults;
never commit `.env` or provider credentials.

### Exercise the reliable pipeline

```powershell
docker compose exec api python -m fluxguard.apps.demo
```

This submits an order twice with one idempotency key, waits for normal outbox publication and
projection, redelivers the exact event, and verifies one logical business effect.

### Run tests and checks

```powershell
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest -q -m "not integration"
```

For PostgreSQL/Redpanda integration tests:

```powershell
$env:RUN_INTEGRATION = "1"
$env:RUN_BENCHMARK_INTEGRATION = "1"
$env:DATABASE_URL = "postgresql+psycopg://fluxguard:fluxguard_local_only@localhost:15432/fluxguard"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
$env:BENCHMARK_POSTGRES_ADMIN_URL = "postgresql+psycopg://fluxguard:fluxguard_local_only@localhost:15432/postgres"
$env:BENCHMARK_KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
.\.venv\Scripts\python.exe -m pytest -q tests/integration --ignore=tests/integration/test_benchmark_demo.py
```

CI runs the same bounded recovery-safety integrations. The complete end-to-end demo remains an
explicit manual workflow.

### Run the isolated recovery demo

With the Compose images built and PostgreSQL/Redpanda healthy:

```powershell
docker compose run --rm --no-deps `
  -v "${PWD}:/workspace" -w /workspace `
  -e BENCHMARK_POSTGRES_ADMIN_URL=postgresql+psycopg://fluxguard:fluxguard_local_only@postgres:5432/postgres `
  -e BENCHMARK_KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 `
  api python -m fluxguard.apps.benchmark --mode end-to-end-demo --output-dir build/demo
```

The runner creates and removes only its owned database, topic, and consumer groups. It writes:

```text
build/demo/<run-id>/
  benchmark-<run-id>.json
  operational-summary-<run-id>.json
  manifest.json
```

Inspect and verify the bundle without running another benchmark:

```powershell
.\.venv\Scripts\python.exe -m fluxguard.apps.observe_benchmark build/demo/<run-id>/benchmark-<run-id>.json --format text
.\.venv\Scripts\python.exe -m fluxguard.apps.verify_benchmark build/demo/<run-id> --format json
```

Verification checks file sizes and SHA-256 hashes, Pydantic schemas, run IDs, evidence classes,
outcome reconstruction, audit consistency, and sensitive-data rejection.

### Run the hosted demo

In GitHub, open **Actions → Isolated end-to-end demo → Run workflow**, select the target branch, and
run it. The `workflow_dispatch` job creates a unique Compose project, uses a masked workflow-scoped
database credential, verifies the bundle before upload, and removes its owned services and volumes.
It receives no model-provider credentials and performs no LLM request.

## Testing and safety model

The Milestone 5 final regression executed the complete unit/integration suite in Linux Docker:
177 tests passed, migrations reached `0006_recovery_execution`, and Ruff lint/format checks passed.

- Unit tests cover contracts, metric denominators, policy decisions, malformed outputs, artifact
  verification, and idempotency.
- PostgreSQL integration tests prove constraints, rollback, append-only audit, approval binding,
  stale-state rejection, and transactional execution.
- Redpanda integration tests exercise actual publication, consumption, quarantine, offset behavior,
  duplicate delivery, and run-scoped resource cleanup.
- Bounded race tests exercise executor/executor, consumer/executor, and publisher/executor conflicts.
- Simulation results, deterministic fixtures, live-provider attempts, historical results, and real
  integrations remain separate evidence classes.
- Any invariant failure, duplicate effect, undeclared write, unexpected publication, audit mismatch,
  or sensitive artifact causes verification to fail.

## APIs and operational gates

The public runtime surface is intentionally small:

- `POST /api/v1/orders` requires `Idempotency-Key`.
- `GET /health/live` and `GET /health/ready` expose process and database health.
- `GET /metrics` exposes process-local counters.
- `GET /docs` exposes the FastAPI OpenAPI UI.

Incident, investigation, and recovery-review APIs require explicit development-only flags. These
flags are exposure gates, not production authentication. Approval still uses a deny-by-default
authorizer, and there is no HTTP recovery-execution endpoint or production recovery worker.

## Current limitations

- Production approval and recovery execution are disabled because no production identity provider,
  authorization policy, least-privilege recovery role, or execution scheduler is configured.
- The investigator shares the application database role; mutation is prevented by the closed tool
  layer rather than a dedicated read-only database role.
- Historical live-provider evaluation completed only 1 of 10 attempts. The one validated diagnosis
  appropriately reported `INSUFFICIENT_EVIDENCE`; the sample establishes neither provider
  reliability nor definitive root-cause accuracy.
- The technical demo uses deterministic evidence inspection and makes no live AI request.
- The publisher-failure scenario is controlled fault injection, not a broker outage.
- Crash-after-commit coverage simulates lost acknowledgement; it is not a process-kill test.
- PostgreSQL and Redpanda run as single local nodes. Failover, replication behavior, network
  partitions, production load, and production availability remain unproven.
- Process-local counters are not a production monitoring system, and no frontend/operator console
  is implemented.

## Technology stack

- Python 3.12, FastAPI, Uvicorn, Pydantic, and pydantic-settings
- PostgreSQL 16, SQLAlchemy 2, psycopg 3, and Alembic
- Redpanda and confluent-kafka
- LangGraph, Google GenAI SDK, and HTTPX-based OpenRouter integration
- structlog
- pytest, Ruff, Docker Compose, and GitHub Actions

## Documentation

| Document | Scope |
| --- | --- |
| [Architecture](docs/architecture.md) | Pipeline flow, reliability boundaries, invariants, investigation, and recovery architecture |
| [Architecture diagrams](docs/architecture-diagrams.md) | Components, trust boundaries, and the verified recovery sequence |
| [Demo walkthrough](docs/demo-walkthrough.md) | Two-minute technical narrative, local and hosted reproduction, verified results, and limitations |
| [Engineering decisions](docs/decisions.md) | Transactional outbox, Kafka client, incident, investigation, and dependency decisions |
| [Incident model](docs/incident-model.md) | Failure taxonomy, evidence, correlation, impact, and API safety |
| [Fault injection](docs/fault-injection.md) | Development-only real-Redpanda scenarios and shutdown steps |
| [Investigation design](docs/investigation-design.md) | Bounded agent, tools, grounding, providers, and historical evaluation limits |
| [Recovery design](docs/milestone-4-recovery-design.md) | Plans, policy, verifier, approval, executor, locks, and invariants |
| [Recovery operations](docs/milestone-4-recovery-operations.md) | Safe retries, rollback response, escalation, and production blockers |
| [Benchmark and demo design](docs/milestone-5-design.md) | Evidence classes, denominators, isolation, observability, demo, and limitations |
| [Milestones](docs/milestones.md) | Implemented capability history and remaining work |
