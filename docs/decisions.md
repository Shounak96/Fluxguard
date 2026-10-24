# Engineering decisions

## ADR-001: Transactional outbox

Direct dual writes to PostgreSQL and Kafka can lose or orphan events. The API therefore commits the
domain write and outbox record in one database transaction. Delivery is at least once; consumer
idempotency handles repetition.

## ADR-002: Synchronous SQLAlchemy with psycopg

Milestone 1 has small, independently scaled processes and no demonstrated need for async database
complexity. SQLAlchemy 2's explicit transaction scopes keep correctness reviewable. The outbox is
an independent background process even though each batch operation is synchronous.

## ADR-003: confluent-kafka

The maintained librdkafka-backed client supports Redpanda's Kafka protocol, delivery callbacks,
manual offset commit, `acks=all`, and idempotent producer mode. Producer idempotence reduces broker-
level duplicates but does not change the system's at-least-once contract.

## ADR-004: Explicit v1 contract

The topic, event type, and Pydantic contract are versioned. Unknown versions go to quarantine rather
than being guessed or autonomously repaired.

## ADR-005: Durable poison-event handling

Malformed/permanently invalid records are stored before their offset is treated as handled.
Infrastructure errors are retried without committing. Quarantine preserves bounded raw input plus
broker coordinates and error detail for later recovery work.

## ADR-006: Minimal observability stack

JSON logs, health/readiness probes, context identifiers, and counters meet the milestone need without
the resource and maintenance cost of Prometheus/Grafana. Metrics are intentionally process-local for
now.

## Dependency policy

Runtime and development dependencies are exactly pinned in `pyproject.toml`; PostgreSQL and Redpanda
images are pinned in Compose. Renovation is a deliberate tested change. No paid dependency, cloud
service, or credential is required.
