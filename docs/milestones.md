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

## Later milestones (not implemented)

- failure detection and incident evidence collection;
- bounded LLM investigation agents;
- recovery-plan policy and simulation;
- invariant gates and safe event replay orchestration;
- recovery evaluation and operator UI.

LLM behavior, autonomous schema repair, recovery execution, cloud deployment, and a monitoring
stack remain outside Milestones 1 and 2.
