# Milestones

## Milestone 1 — reliable order event pipeline

Implemented:

- validated, idempotent order submission and exact Decimal totals;
- transactional outbox with acknowledged Kafka-compatible publication;
- schema-checked, invariant-aware, idempotent projection consumer;
- durable malformed-event quarantine and transient retry behavior;
- Alembic schema, Docker Compose runtime, unit/integration tests, CI, and Windows instructions.

## Milestone 2 — incident detection and evidence

Implemented:

- deterministic schema, envelope, invariant, poison, retry, and publication-failure detection;
- correlated durable incidents and bounded, redacted per-occurrence evidence;
- transactionally coordinated quarantine/evidence and threshold retry tracking;
- static pipeline impact analysis and development-restricted read APIs;
- controlled real-Redpanda fault scenarios with production safeguards.

## Milestone 3 â€” evidence-grounded AI investigation

Implemented:

- provider-independent OpenRouter and Gemini access with free-model enforcement and bounded retries;
- a bounded LangGraph loop with competing hypotheses and approved read-only tools;
- deterministic evidence-citation validation and explicit insufficient/unverified outcomes;
- durable queued runs, background execution, migration `0003`, and restricted APIs;
- fake-model tests plus real PostgreSQL persistence coverage without live-model calls in CI.

The exploratory historical benchmark completed 1 of 10 live investigations. The validated result
used genuine evidence and appropriate `INSUFFICIENT_EVIDENCE`, but no definitive root-cause result
is evaluable. The sample is too small to establish provider reliability or diagnostic accuracy.

## Later milestones (not implemented)

- recovery-plan policy and simulation;
- invariant gates and safe event replay orchestration;
- recovery evaluation and operator UI.

Autonomous schema repair, recovery execution, cloud deployment, and a monitoring stack remain
outside Milestones 1â€“3.
