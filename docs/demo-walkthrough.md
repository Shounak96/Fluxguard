# FluxGuard end-to-end demo walkthrough

This two-minute walkthrough shows how FluxGuard handles one recoverable pipeline failure and one
unsafe event in an isolated PostgreSQL and Redpanda environment. It exercises the real event,
incident, verification, recovery, and audit paths. It does **not** enable production recovery or
call a live LLM.

## What the demonstration proves

The demo keeps recovery authority separate from diagnosis. Persisted evidence feeds a deterministic
policy and a PostgreSQL-backed dry run; an internal executor can apply only an approved, declared
write set. A second lane proves that an unsupported event is retained and rejected rather than
replayed.

## Two-minute engineering walkthrough

| Time | Stage | What happens and why it matters |
| --- | --- | --- |
| 0:00 | **A. Normal transactional-outbox processing** | FastAPI commits an order and outbox record together. The ordinary publisher sends the event to run-scoped Redpanda, and the idempotent consumer creates one projection. |
| 0:12 | **B. Injected publisher failure** | A controlled failing publisher exhausts bounded attempts for a second outbox record. This is not a broker outage. |
| 0:24 | **C. Persisted incident and evidence** | FluxGuard persists an incident and owned evidence whose digest binds recovery to the exact event bytes; it never reconstructs a missing payload. |
| 0:35 | **D. Deterministic recovery planning** | The closed policy proposes an eligible outbox reschedule from persisted state, without model-authorized mutation. |
| 0:46 | **E. Rollback-only PostgreSQL verification** | The verifier reloads authoritative state, simulates only the declared write set, checks postconditions, rolls back, then separately records an immutable decision. |
| 0:59 | **F. Default-deny runtime authorization** | The normal runtime authorizer denies approval. Production approval and execution remain disabled. |
| 1:09 | **G. Test-scoped approval and execution** | Inside the isolated process, a trusted test identity approves the exact plan and persisted bindings. The internal executor rechecks them and reschedules only eligible outbox fields. |
| 1:23 | **H. Ordinary publisher/consumer continuation** | The executor never publishes directly. The ordinary publisher and consumer continue the event path, producing exactly one marker and projection. |
| 1:35 | **I. Unsafe-schema rejection** | A schema-version-99 event is quarantined with incident evidence. Replay verification rejects it, with no approval, execution, or projection. |
| 1:48 | **J. Independent invariants and audit validation** | The harness checks cardinality, declared writes, outbox effects, Kafka watermarks, invariants, and immutable audit reconciliation before accepting the bundle. |

## Run the isolated demo locally

Prerequisites are Python 3.12+, Docker Desktop with Compose v2, installed development dependencies,
and built Compose images with PostgreSQL and Redpanda healthy. From the repository root, run the
documented bounded demo command:

```powershell
docker compose run --rm --no-deps `
  -v "${PWD}:/workspace" -w /workspace `
  -e BENCHMARK_POSTGRES_ADMIN_URL=postgresql+psycopg://fluxguard:fluxguard_local_only@postgres:5432/postgres `
  -e BENCHMARK_KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 `
  api python -m fluxguard.apps.benchmark --mode end-to-end-demo --output-dir build/demo
```

The runner creates a uniquely named disposable database plus run-scoped topic and consumer groups.
It stops owned workers and removes only those resources on success, failure, or interruption. The
result is written under `build/demo/<run-id>/` as:

- `benchmark-<run-id>.json` — authoritative benchmark report, schema version 2
- `operational-summary-<run-id>.json` — derived operational summary, schema version 1
- `manifest.json` — bundle manifest, schema version 1, including byte sizes and SHA-256 checksums

Inspect and independently verify a completed local run without executing another demo:

```powershell
$run = Get-ChildItem build/demo -Directory | Sort-Object LastWriteTime -Descending | Select-Object -First 1
$runId = $run.Name
.\.venv\Scripts\python.exe -m fluxguard.apps.observe_benchmark "$($run.FullName)\benchmark-$runId.json" --format text
.\.venv\Scripts\python.exe -m fluxguard.apps.verify_benchmark $run.FullName --format json
```

The verifier fails closed on a missing or modified file, schema or run-ID mismatch, inconsistent
outcome/audit summary, safety assertion failure, or sensitive field.

## Reproduce with GitHub Actions

1. Open the repository's **Actions** tab and select **Isolated end-to-end demo**. The workflow is
   defined in [`.github/workflows/demo.yml`](../.github/workflows/demo.yml).
2. Select **Run workflow**, choose `main`, and start the manually dispatched run.
3. After the job succeeds, download the **fluxguard-end-to-end-demo** artifact from the run page.
4. Extract the artifact inside the repository, then inspect its schema-v2 report, schema-v1
   operational summary, and schema-v1 manifest.
5. Run the existing verifier against the extracted run directory:

```powershell
Expand-Archive .\fluxguard-end-to-end-demo.zip -DestinationPath .\downloaded-demo
$run = Get-ChildItem .\downloaded-demo -Directory | Select-Object -First 1
$runId = $run.Name
.\.venv\Scripts\python.exe -m fluxguard.apps.observe_benchmark "$($run.FullName)\benchmark-$runId.json" --format text
.\.venv\Scripts\python.exe -m fluxguard.apps.verify_benchmark $run.FullName --format json
```

The GitHub workflow uses a unique Compose project, disposable PostgreSQL and Redpanda services, a
masked workflow-scoped database credential, a 15-minute job bound, and unconditional owned-resource
cleanup. It receives no model-provider credential and makes no LLM request.

## Verified hosted result

The following values are measurements from the completed hosted run, not illustrative output:

| Field | Observed value |
| --- | ---: |
| Run ID | `2c416562-e8b9-41dd-ba4b-05d41d63e6a2` |
| Git SHA | `3aa4d7606169344acb9863abf1bba1df13469070` |
| `RECOVERED` | 1 |
| `SAFELY_REJECTED` | 1 |
| Audit consistency | 2/2 |
| Invariant violations | 0 |
| Duplicate business effects | 0 |
| Recoverable path | 4,779.858 ms |
| Recovery execution stage | 39.274 ms |

The report files carry correlation and audit-trace identifiers—such as incident, plan,
verification, approval, execution, and lifecycle-event IDs—plus sanitized reason codes and timing
samples. They intentionally exclude raw event payloads, credentials, connection strings, and raw
model/provider responses.

## Scope and limitations

- The recoverable fault is an injected failing publisher, not an actual Redpanda outage.
- Approval uses a trusted identity available only inside the isolated test/demo process.
- Investigation is deterministic evidence inspection; the hosted demo performs no live LLM call.
- Results are local, single-node observations from a small sample, not production reliability,
  throughput, availability, or latency objectives.
- Crash-after-commit coverage elsewhere in the project is simulated, not an actual process kill.
- Production approval and recovery execution remain disabled; production authentication,
  scheduling, monitoring, and operational readiness are future work.

For the component, trust-boundary, and recovery-sequence views behind this walkthrough, see the
[architecture diagrams](architecture-diagrams.md). For benchmark denominators and isolation design,
see the [Milestone 5 design](milestone-5-design.md).
