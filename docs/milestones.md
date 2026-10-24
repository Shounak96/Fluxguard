# Milestones

## Milestone 1 — reliable order event pipeline

Implemented:

- validated, idempotent order submission and exact Decimal totals;
- transactional outbox with acknowledged Kafka-compatible publication;
- schema-checked, invariant-aware, idempotent projection consumer;
- durable malformed-event quarantine and transient retry behavior;
- Alembic schema, Docker Compose runtime, unit/integration tests, CI, and Windows instructions.

## Later milestones (not implemented)

- failure detection and incident evidence collection;
- bounded LLM investigation agents;
- recovery-plan policy and simulation;
- invariant gates and safe event replay orchestration;
- recovery evaluation and operator UI.

LLM behavior, autonomous schema repair, cloud deployment, and a monitoring stack are intentionally
outside Milestone 1.
