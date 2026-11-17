# Milestone 5 benchmark, observability, and demo design

## Scope and safety boundary

Milestone 5 measures and demonstrates the existing pipeline, incident, investigation, and recovery
behavior. It does not add a recovery action, weaken deterministic verification, activate production
approval or execution, or require a model call. The default benchmark is local and uses the existing
Docker Compose PostgreSQL and Redpanda services with disposable, run-scoped data.

All results identify their evidence class. These classes are reported independently and are never
combined into one success rate:

- `REAL_INTEGRATION`: real PostgreSQL transactions and, where the scenario requires it, real
  Redpanda production/consumption through the local Compose services;
- `DETERMINISTIC_SIMULATION`: a real integration path with a controlled exception, lost-response
  boundary, or other simulated process condition, but without the claimed real-world failure;
- `DETERMINISTIC_FIXTURE`: contract, policy, scoring, or report behavior without a complete real
  service path;
- `LIVE_PROVIDER`: an actual external investigation-provider attempt, reported separately from
  recovery safety and disabled by default.

Historical records are labeled `HISTORICAL` and are not presented as a new benchmark execution.
No fixture or simulation may be relabeled as a genuine run.

## Benchmark methodology

### Reproducible local environment

The benchmark runner performs these bounded steps:

1. Record the commit SHA and whether the working tree is dirty. Local development runs are allowed
   from a dirty tree, and that state remains visible in the report.
2. Record sanitized Python, FluxGuard, Docker, Compose, PostgreSQL image, and Redpanda image
   versions. It must record configuration values that affect behavior, but never database
   passwords, API keys, connection strings, event payloads, or environment dumps.
3. Start only the existing required Compose dependencies. Application workers are started and
   stopped per scenario so consumers and publishers cannot race a fixture unintentionally.
4. Create a unique benchmark run ID, disposable PostgreSQL database, Kafka topic suffix, consumer
   group suffix, idempotency keys, event IDs, and execution IDs. A fixed scenario seed determines
   fixture content; UUIDs generated for isolation are recorded rather than treated as deterministic
   outputs.
5. Apply Alembic through the current head only to the newly created owned database. Connections
   are restricted to approved local Docker hosts and the PostgreSQL maintenance database. The test
   authorizer is constructed only inside the isolated scenario process.
6. Capture a database baseline for every table in the declared read/write scope. For each trial,
   capture pre-state, expected write set, post-state, Kafka publication delta, and immutable audit
   history. Unrelated-table equality is an assertion, not merely a logged observation.
7. Use monotonic clocks for durations and UTC timestamps only for correlation. Warm-up trials, if
   later added, are marked and excluded from measured denominators.
8. Restore development and fault-injection gates to disabled, stop only benchmark-owned worker
   processes, and remove only the disposable database and run-scoped broker resources. Never remove
   Compose volumes or unrelated Docker resources.

The default run is bounded: one trial for each serial scenario and exactly three trials for each
declared concurrency scenario. A command-line trial override must have a small configured maximum
and must be recorded. There are no unbounded stress loops or automatic retries of a failed trial.

### Trial outcome model

Every trial has exactly one primary outcome:

- `RECOVERED`: the expected eligible mutation committed with its exact declared effect;
- `VERIFIED_NO_OP`: an identical prior business effect was proven and no mutation was repeated;
- `SAFELY_REJECTED`: the unsafe, stale, unauthorized, or conflicting action failed closed;
- `OPERATIONAL_FAILURE`: infrastructure or harness failure prevented a safety conclusion;
- `INVARIANT_VIOLATION`: a postcondition or business invariant was violated;
- `DUPLICATE_BUSINESS_EFFECT`: projection, processed marker, outbox transition, or equivalent effect
  occurred more than once;
- `AUDIT_MISMATCH`: business and immutable audit state disagree.

An operational failure is not an unsafe-recovery rejection and is not scored as incorrect recovery
behavior. A no-op is not counted as a successful mutation. A trial with any invariant violation,
duplicate effect, undeclared write, unexpected Kafka publication, or audit mismatch cannot also be
reported as successful.

### Deterministic scenario matrix

| Case | Evidence class | Expected result | Required assertions |
|---|---|---|---|
| Healthy order through API, outbox, Redpanda, and consumer | `REAL_INTEGRATION` | Baseline only; no recovery | One order, outbox event, processed marker, and projection; correct totals |
| Approved valid replay with exact authoritative bytes | `REAL_INTEGRATION` | `RECOVERED` | One processed marker and projection; declared write set only; success audit commits |
| Identical event already applied | `REAL_INTEGRATION` | `VERIFIED_NO_OP` | Cardinalities unchanged; immutable no-op audit; retry returns same result |
| Approved unpublished outbox reschedule | `REAL_INTEGRATION` | `RECOVERED` | Only scheduling fields change; executor publishes zero Kafka records |
| Unsupported schema version | `REAL_INTEGRATION` | `SAFELY_REJECTED` | Quarantine retained; no projection or replay; typed reason |
| Malformed JSON | `REAL_INTEGRATION` | `SAFELY_REJECTED` | Exact retained bytes/hash; no synthesized event or projection |
| Invalid order or line total | `REAL_INTEGRATION` | `SAFELY_REJECTED` | Financial invariants fail; all business tables unchanged |
| Poison-message marker | `REAL_INTEGRATION` | `SAFELY_REJECTED` | Poison event cannot be replayed; quarantine remains |
| Missing or incident-foreign evidence | `REAL_INTEGRATION` | `SAFELY_REJECTED` | Ownership failure; no verification or business mutation falsely succeeds |
| Conflicting projection or processed-event identity | `REAL_INTEGRATION` | `SAFELY_REJECTED` | Existing healthy record unchanged; conflict is typed |
| Stale state digest, expired approval, or version drift | `REAL_INTEGRATION` | `SAFELY_REJECTED` | Execution rechecks current state and versions; append-only failure/expiry history |
| Two executors for one event, three bounded trials | `REAL_INTEGRATION` | One effect; other result is no-op/conflict | At most one business effect and one terminal success/no-op per plan |
| Consumer and executor race, three bounded trials | `REAL_INTEGRATION` | One business effect | Atomic processed-event claim prevents duplicate projection |
| Publisher and executor race, three bounded trials | `REAL_INTEGRATION` | Publish or reschedule wins safely | Published state is never overwritten; executor never publishes directly |
| Retry after committed execution with response treated as lost | `DETERMINISTIC_SIMULATION` | `VERIFIED_NO_OP` or same immutable result | No duplicate effect; explicitly state that no process was killed |
| Injected exception before commit | `DETERMINISTIC_SIMULATION` | `OPERATIONAL_FAILURE` with safe rollback | Business state equals baseline; sanitized failure audit written after rollback |
| Lock timeout or forced serialization conflict | `DETERMINISTIC_SIMULATION` | Typed operational conflict | Bounded latency, no partial writes, safe retry guidance |

The existing fault taxonomy supplies unsupported-schema, malformed, invariant-invalid, poison,
transient-consumer, and outbox-publication cases. A scenario records whether it used the public
fault-injection path, an integration fixture, or a controlled monkeypatch. The transient scenario
also proves that offsets are not advanced past an unpersisted failure and that eventual application
still creates only one projection.

## Metrics and denominators

Each metric is emitted as `{numerator, denominator, unavailable, rate_percent}`. `rate_percent` is
null when the denominator is zero. Counts are integers even when lower is better.

| Metric | Numerator | Denominator | Unavailable |
|---|---|---|---|
| Safe recovery success | Eligible mutating trials ending in `RECOVERED` with all safety assertions true | Eligible mutating trials that reached execution | Eligible cases that could not reach execution because of operational setup failure |
| Verified no-op correctness | Expected-no-op trials producing no business mutation and correct immutable audit | Trials whose expected result is no-op | No conclusive state or audit snapshot |
| Unsafe recovery rejection | Expected-rejection trials rejected before forbidden mutation, with correct reason and audit | All genuine expected-rejection trials attempted | Operational failures that prevented a policy or execution decision |
| Duplicate-effect incidence | Conclusive trials with one or more duplicate business effects | Conclusive recovery and concurrency trials | Trials without a conclusive post-state |
| Invariant-violation incidence | Conclusive trials with any failed declared business invariant or undeclared write | Conclusive recovery trials | Trials without a conclusive post-state |
| Audit consistency | Trials whose lifecycle, verification, approval, execution events, and committed business outcome agree | Trials that reached a stage requiring persisted audit | Audit could not be read because of infrastructure failure |
| Concurrency safety | Conclusive concurrency trials with an allowed winner/no-op/conflict outcome and at most one effect | Conclusive concurrency trials | Timed-out trials without a provable final state |
| Operational completion | Trials that reached their expected terminal outcome | All trials started in the same evidence class | Always zero; failures remain in the denominator |
| Live provider availability | Live attempts producing a strictly validated diagnosis | Live provider attempts | Always zero; transport and validation failures remain in the denominator |

`duplicate_effect_count` and `invariant_violation_count` are also reported as absolute counts. Their
desired value is zero; they are never converted into a flattering "accuracy" metric.

Investigation diagnostic quality retains the existing five dimensions: observed-fault recognition,
root-cause correctness, evidence-citation validity, appropriate uncertainty, and schema compliance.
Those rates are computed only over completed, relevant, validated diagnoses. Live provider
availability is separate, missing diagnoses are unavailable rather than incorrect, and deterministic
fake-model results are never reported as live model performance.

### Latency

Record separate durations for incident persistence, deterministic inspection or investigation,
plan creation, dry-run verification, approval, execution, final-state validation, and end-to-end
trial completion. A stage summary contains `count`, `unavailable`, `min_ms`, `median_ms`, `p95_ms`,
and `max_ms`; no percentile is emitted for an empty cohort. Use the nearest-rank p95 over raw
unrounded durations, then round displayed milliseconds. Report genuine integration, simulation,
fixture, and live-provider distributions separately. Do not infer production capacity or an SLO
from three local concurrency trials.

## Machine-readable report

Benchmark report schema version 2 is canonical JSON containing run/configuration/environment data,
validated scenario results, evidence-class cohorts, explicit metric denominators, stage latencies,
audit references, and limitations. Operational-summary schema version 1 is deterministically rebuilt
from that report. The Pydantic contracts are frozen, reject extra fields, and enforce cross-field
consistency. The bundle manifest records both schemas, sizes, and SHA-256 checksums.

The report contains identifiers and digests needed for audit correlation but no raw event bytes,
customer data, credentials, provider responses, exception tracebacks, database URLs, or complete
environment variables. Sanitized error data is limited to category, stage, retryability, and a
bounded safe message. JSON output and a concise terminal table are derived from the same validated
report object.

## Lightweight observability

Reuse `structlog`, context variables, process-local counters, and PostgreSQL records:

- bind `benchmark_run_id`, `trial_id`, `scenario`, and `coverage` in the benchmark process;
- propagate existing `correlation_id`, `event_id`, and `incident_id`, and include `plan_id`,
  `verification_id`, `approval_id`, and `execution_id` where available;
- emit bounded stage events such as `benchmark_trial_started`, `incident_observed`,
  `recovery_verification_completed`, `recovery_execution_completed`,
  `benchmark_assertions_completed`, and `benchmark_trial_completed`;
- record elapsed milliseconds, typed outcome/reason, declared/applied write-set counts, and audit
  sequence counts, but not raw payloads or authorization material;
- treat immutable PostgreSQL history and business rows as the source of truth. Process counters and
  container logs are diagnostic aids, not benchmark evidence by themselves;
- query health endpoints, PostgreSQL readiness, Redpanda health, consumer-group position, and
  worker exit state at explicit checkpoints;
- retain a sanitized JSON report and optional captured JSON log stream under a git-ignored build
  directory. No paid monitoring backend is required.

The first implementation should not add a metrics server, tracing collector, production dashboard,
or high-cardinality production labels. A future exporter can consume the same stable event and
report contracts without changing recovery semantics.

### Implemented operational summary

Benchmark report schema version 2 records sanitized incident, plan, verification, approval, and
execution references captured from immutable PostgreSQL history before the disposable database is
removed. It records ordered lifecycle state/reason references and an explicit reconciliation state;
missing rows remain `UNAVAILABLE` rather than being synthesized. Operational-summary schema version
1 then separates successful execution, verified no-op, safe rejection, expected concurrency
conflict, expected injected failure, unexpected operational failure, and safety assertion failure.

Summarize an existing report without executing another benchmark:

```powershell
fluxguard-observe-benchmark build/demo/<run-id>/benchmark-<run-id>.json --format text
fluxguard-observe-benchmark build/demo/<run-id>/benchmark-<run-id>.json --format json
```

The Slice 3 report's 1,144.29 ms end-to-end p95 cannot be assigned to a single recorded stage. Its
tail cases had execution measurements well below the end-to-end duration, while Kafka watermark
queries, snapshot/assertion work, and setup were included only in end-to-end time. Final-state
validation had no independent samples. This suggests uninstrumented boundary work contributed to
the tail, but the existing data cannot distinguish those contributors precisely; no benchmark was
rerun to infer a cause.

## End-to-end technical demo

The demo is a scripted, non-interactive run with a run ID and bounded timeouts. It has a success
lane and a rejection lane so it cannot imply every failed event is repairable.

1. **Preflight:** show the commit, migration head, service health, disabled production recovery,
   and the fixture-only trusted identity boundary. Create run-scoped data and enable development
   fault gates only for the isolated demo processes.
2. **Normal flow:** submit an order with a unique idempotency key. Show the order/outbox transaction,
   Redpanda acknowledgement, processed marker, and single projection with verified totals.
3. **Recoverable fault:** with the publisher lifecycle controlled by the fixture, create an outbox
   publication failure and persisted incident/evidence while the row remains unpublished. Show that
   normal healthy rows are unchanged.
4. **Inspection:** load deterministic incident impact and owned evidence. Optionally display a
   pre-existing investigation as historical context; do not imply it was generated during the demo.
   A live investigation is an optional, separately authorized future variant, never a prerequisite.
5. **Proposal and dry run:** build the closed `RESCHEDULE_OUTBOX_EVENT` plan from authoritative
   state, persist it, run the rollback-only verifier, and show `ELIGIBLE`, the declared write set,
   zero Kafka publications, and before/after business-table equality.
6. **Approval boundary:** first demonstrate denial by the runtime authorizer. Then, inside the
   isolated demo fixture only, inject the existing trusted test identity and approve the exact plan,
   verification, state/evidence digests, versions, and expiry. No caller-supplied identity is used.
7. **Controlled recovery:** execute internally. Show that only the outbox scheduling fields changed
   and the executor published nothing. Start the ordinary publisher to publish the row, then show
   the ordinary consumer path and final audit history.
8. **Unsafe lane:** inject an unsupported-schema or invariant-invalid event. Show the incident,
   quarantine evidence, deterministic `INELIGIBLE` decision, typed rejection, zero projection, and
   retained quarantine. There is no approval or bypass.
9. **Final proof:** query cardinalities, financial invariants, unexpected publication count,
   unrelated-row digest, plan/verification/approval/execution history, and report outcome. Restore
   all development gates to disabled and stop only demo-owned worker processes.

The terminal narrative and JSON result must agree. Commands should print IDs and bounded summaries,
not payloads. The demo must abort rather than silently substitute fixtures if PostgreSQL, Redpanda,
or a required worker is unavailable.

### Single-command isolated demo

With the existing Compose PostgreSQL and Redpanda services healthy, run from PowerShell:

```powershell
docker compose run --rm --no-deps `
  -v "${PWD}:/workspace" -w /workspace `
  -e BENCHMARK_POSTGRES_ADMIN_URL=postgresql+psycopg://fluxguard:fluxguard_local_only@postgres:5432/postgres `
  -e BENCHMARK_KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 `
  api python -m fluxguard.apps.benchmark --mode end-to-end-demo --output-dir build/demo
```

The command creates a uniquely named disposable database, topic, and consumer-group namespace. It
prints the run directory `build/demo/<run-id>`, containing `benchmark-<run-id>.json` (schema v2),
`operational-summary-<run-id>.json` (schema v1), and `manifest.json` (schema v1), then removes only
its owned database and Kafka topic. The manifest records file sizes and SHA-256 checksums. The report
should contain one genuine recovered outbox lane
and one safely rejected unsupported-schema lane, zero invariant violations or duplicate effects,
and consistent audit correlation. Investigation is labeled deterministic evidence inspection; no
model is called. The ordinary publisher performs all Kafka publication, while the recovery executor
only reschedules the verified outbox row. Runtime approval is first denied, and the process-local
test authorizer is used only after that boundary is demonstrated.

Inspect and independently verify the packaged output without executing another demo:

```powershell
fluxguard-observe-benchmark build/demo/<run-id>/benchmark-<run-id>.json --format text
fluxguard-verify-benchmark build/demo/<run-id> --format json
fluxguard-verify-benchmark build/demo/<run-id> --format text
```

JSON is authoritative. Verification checks the manifest, file set, sizes and checksums, Pydantic
schemas, run IDs, evidence classes, rebuilt outcome summary, audit reconciliation, and sensitive
field/value rejection. A mismatch exits nonzero. Bundle creation uses a temporary run directory and
removes only that partial directory if writing is interrupted.

The manual `Isolated end-to-end demo` GitHub Actions workflow uses `workflow_dispatch` only. It
starts Compose PostgreSQL and Redpanda, runs one bounded demo, verifies the bundle before upload, and
stops its services without deleting shared or production resources. It supplies no provider keys
and performs no LLM request.

The controlled publication fault is an injected failing publisher, not an actual broker outage.
Approval uses a process-local isolated test identity. No live LLM investigation is performed.
Crash-after-commit coverage is simulated rather than a real process kill. Production approval and
execution remain disabled, and these small local samples are reproducibility evidence—not production
availability, latency, or reliability SLAs.

### Observed exploratory local results

The bounded recovery-safety run produced 19 genuine local integration trials and one deterministic
simulation: 8/8 eligible recoveries completed, 7/7 unsafe actions were rejected, and all 9 bounded
concurrency trials preserved safety. No invariant violation or duplicate business effect was
observed. The measured end-to-end p95 was 1,144.29 ms, without enough stage attribution to assign a
single cause.

The latest packaged local demo produced one `RECOVERED` and one `SAFELY_REJECTED` outcome with 2/2
consistent audit traces, zero invariant violations, and zero duplicate effects. Its measured lane
durations were 5,788.20 ms and 238.41 ms. These are small, single-node exploratory observations,
not capacity measurements, availability claims, or production SLAs.

## Implementation slices and acceptance criteria

### Slice 1: report contracts and metric rules

- Extend or replace the current recovery safety report with versioned frozen Pydantic contracts.
- Implement cohort separation, numerator/denominator/unavailable metrics, latency summaries, and
  deterministic JSON plus terminal rendering.
- Unit-test ordering, empty denominators, unavailable values, percentile rules, redaction, and the
  prohibition on mixing evidence classes.

Acceptance: the same inputs produce byte-identical JSON apart from explicitly modeled run IDs and
timestamps; fixture results cannot enter genuine or live cohorts.

### Slice 2: isolated benchmark orchestration

- Add a CLI that performs preflight, creates run-scoped database/broker configuration, applies
  migrations, controls workers, and guarantees fixture teardown.
- Reuse current integration fixtures rather than creating a second persistence or execution path.
- Add hard bounds for timeouts, serial trials, concurrency trials, and cleanup scope.

Acceptance: an interrupted run cannot activate production recovery, delete a volume, or affect a
non-benchmark database/topic; environment and migration metadata are recorded without secrets.

### Slice 3: recovery safety scenarios

- Convert the existing replay, no-op, rejection, rollback, and race coverage into reusable scenario
  functions that return validated observations.
- Add the exact allowed/rejected matrix above and three bounded trials for each concurrency case.
- Independently query post-state, Kafka deltas, write sets, and immutable audit history.

Acceptance: any duplicate effect, invariant violation, unexpected Kafka publication, undeclared
write, or audit mismatch fails the report; simulations are labeled and isolated from genuine runs.

### Slice 4: operational observability

- Add benchmark context binding and stable structured event names at orchestration boundaries.
- Derive stage durations and report data from monotonic measurements and persisted records.
- Add log-redaction and report-schema tests; do not add an external monitoring service.

Acceptance: a run can be correlated from scenario through incident, evidence, plan, verification,
approval, execution, and final state without logging a payload, credential, or trusted identity
material.

### Slice 5: reproducible demo

- Build the scripted success and rejection lanes with concise checkpoints and a machine-readable
  result.
- Keep the trusted test authorizer in the fixture process and prove the normal runtime authorizer
  denies the same approval/execution path.
- Document Windows invocation through Linux Docker and the expected output categories.

Acceptance: a fresh local checkout with Docker can reproduce the demo without a model key or paid
service, and the final JSON agrees with PostgreSQL and Redpanda observations.

### Slice 6: regression and CI packaging

- Run report/unit tests in the normal unit job and bounded genuine integration scenarios in the
  existing Linux integration job.
- Keep the longer full benchmark and demo as explicit local/manual commands unless runtime is shown
  to fit the existing CI bound reliably.
- Validate migrations, Ruff, report schema, cleanup safety, and documentation commands.

Acceptance: CI exercises metric correctness and representative safety paths without model calls;
full local output clearly identifies trial count and evidence class.

## Risks and limitations

- Local Docker latency and three concurrency trials are reproducibility checks, not throughput,
  capacity, reliability, or production SLO evidence.
- The simulated lost-acknowledgement case does not prove real process-kill, host-loss, broker-loss,
  or multi-node durability. Those require controlled external process orchestration and are labeled
  future work.
- There is no production identity provider, least-privilege recovery role, execution scheduler,
  distributed worker, monitoring backend, or demonstrated production recovery availability.
- The demo test identity proves approval binding only inside an isolated fixture. It is not a
  production authentication design and must not be reachable from an application endpoint.
- Without a live model call, the demo can show deterministic inspection or clearly labeled
  historical investigation data, but cannot claim current provider availability or a newly generated
  diagnosis. Live model metrics require a separately authorized, budgeted cohort.
- A live investigation, if ever enabled, can propose context only. It cannot affect eligibility,
  approval, verification, execution, metrics classification, or safety assertions.
- Redpanda and PostgreSQL run as local single-node services. Broker replication, database failover,
  network partitions, and production load are outside this milestone.
- Fault injection validates named code paths; it cannot establish completeness for unknown failures.
  Operational failures remain visible rather than being converted into diagnostic or recovery
  failures.
