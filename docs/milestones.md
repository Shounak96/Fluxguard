# Implemented milestones

## Milestone 1 — reliable order event pipeline

- Validated, idempotent order submission with exact `Decimal` totals
- Transactional outbox with acknowledged Kafka-compatible publication
- Schema-checked, invariant-aware, idempotent projection consumer
- Durable malformed-event quarantine and transient retry behavior
- Alembic schema, Docker Compose runtime, unit/integration tests, and CI

## Milestone 2 — incident detection and evidence

- Deterministic schema, envelope, invariant, poison, retry, and publication-failure detection
- Correlated durable incidents and bounded, redacted per-occurrence evidence
- Transactionally coordinated quarantine/evidence and threshold retry tracking
- Static pipeline impact analysis and development-restricted read APIs
- Controlled real-Redpanda fault scenarios with production safeguards

## Milestone 3 — evidence-grounded AI investigation

- Provider-independent OpenRouter and Gemini adapters with explicit free-model enforcement
- Bounded LangGraph workflow with competing hypotheses and approved read-only tools
- Strict structured-output parsing and independent evidence-citation validation
- Explicit `INSUFFICIENT_EVIDENCE` and `UNVERIFIED` outcomes
- Durable queued investigations, background execution, and restricted APIs
- Deterministic evaluation harness separating fixtures, historical data, and live-provider attempts

The exploratory historical live-provider evaluation completed one validated diagnosis in ten
attempts. It used verified evidence and appropriate uncertainty but produced no evaluable definitive
root cause. The sample does not establish provider reliability or diagnostic accuracy.

## Milestone 4 — invariant-verified recovery

- Closed, versioned recovery plan and eligibility contracts
- Immutable plans, verification records, approvals, execution records, and lifecycle audit
- PostgreSQL `REPEATABLE READ` rollback-only verification using authoritative state
- Evidence ownership, content hash, state digest, write-set, invariant, and postcondition checks
- Approval binding, expiry, stale-state rejection, and deny-by-default authorization
- Internal idempotent executor with advisory locks and no direct Kafka publication
- Real PostgreSQL concurrency and rollback validation

Production approval and execution remain disabled. No public execution endpoint, unrestricted
worker, or production identity provider exists.

## Milestone 5 — benchmarks, observability, and reproducible demo

- Versioned evidence-classed benchmark reports with explicit metric denominators
- Disposable PostgreSQL/Redpanda runner with owned cleanup and bounded trials
- Real recovery-safety scenarios plus separately classified deterministic simulation
- Audit reconciliation and operational-summary CLI
- Isolated end-to-end recovery and rejection demo
- Schema-validated, checksummed artifact bundles and independent verification CLI
- Manual GitHub Actions demo with workflow-scoped credentials and resource cleanup

The bounded recovery-safety result contains 19 real integration trials and one simulation: 8/8
eligible recoveries, 7/7 unsafe-action rejections, 9/9 safe concurrency trials, and no observed
duplicate business effects or invariant violations. These local single-node results are exploratory,
not production reliability or capacity claims.

## Remaining work

- Production identity provider, authorization policy, and least-privilege database roles
- Production recovery claiming, scheduling, operational monitoring, and incident-resolution policy
- Real process-kill, broker-outage, network-partition, failover, and multi-node durability testing
- A dedicated read-only investigator database role
- Operator-facing workflow or UI, if justified by a separately reviewed security model

Autonomous schema repair, unrestricted replay, arbitrary SQL recovery, cloud deployment, and a
production monitoring stack are not implemented.
