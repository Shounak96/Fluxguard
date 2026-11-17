# Architecture

See [architecture and trust-boundary diagrams](architecture-diagrams.md) for the component map,
authorization boundaries, and eligible/rejected recovery sequences.

## Data flow

```text
client -> FastAPI -> [orders + order_items + outbox_events] (one PostgreSQL transaction)
                                      |
                               outbox publisher
                                      |
                           Redpanda orders.created.v1
                                      |
                                order consumer
                                      |
              [processed_events + order_projections] (one PostgreSQL transaction)
                         or [quarantined_events]
```

API command state and consumer projection state are separate, making eventual consistency visible
and testable. Contracts in `fluxguard/packages/contracts` contain no transport code. Business
calculations live in the API service, while broker loops are thin adapters.

## Reliability boundaries

The order and its outbox event commit together. The publisher holds a row lock while sending,
waits for broker acknowledgment, and only then sets `published_at`. If it crashes after the broker
acknowledges, the event remains pending and will be sent again. `SKIP LOCKED` permits safe future
horizontal publisher concurrency.

The consumer disables auto-commit. For a valid event, the unique `processed_events.event_id` and
projection are written in one transaction; the Kafka offset is committed afterward. A failed
database transaction is retried on the same in-memory record, preventing a later offset commit from
skipping it. Duplicate IDs have no second business effect.

Invalid JSON, unsupported schema versions, contract violations, and failed payload invariants are
written to `quarantined_events`. The `(topic, partition, offset)` constraint makes that operation
idempotent. The offset is committed only after that write succeeds. Raw bytes are UTF-8 when safe,
otherwise base64, and capped at 1 MB.

## Invariants enforced now

- Money is `Decimal`/`NUMERIC(18,2)`, never binary floating point.
- Order total equals the sum of line totals.
- Each line total equals quantity multiplied by unit price.
- Envelope aggregate ID equals payload order ID.
- One API order per idempotency key and canonical request hash.
- One processed marker per event ID and one projection per order ID.
- UTC-aware timestamps at application boundaries.

## Observability

`structlog` emits JSON with UTC timestamp, level, event name, correlation ID, and event ID where
available. `/health/live` and `/health/ready` distinguish process and database health. Lightweight
in-process counters cover created/replayed orders and successful, duplicate, quarantined, and failed
event work. They reset on restart; durable tables and immutable lifecycle records are the audit
source of truth.

## Incident detection extension

Permanent consumer failures commit quarantine, incident correlation, and evidence in one database
transaction before the Kafka offset is committed. Retryable processing failures keep the same
record in the consumer loop and persist bounded retry state; an incident is opened only after the
configured threshold. Outbox failure evidence is committed in the same transaction as the attempt
counter and next retry time.

```text
Consumer classification -> quarantine + incident + evidence -> offset commit
Transient exception      -> retry state -> threshold incident -> retry same offset
Outbox exception          -> attempt/backoff + threshold incident -> retry publication
```

The incident API reads only sanitized evidence. Impact analysis follows the fixed Milestone 2
topology `API -> Outbox -> Redpanda -> Consumer -> Projection`; it is not a generic graph engine.

## Investigation extension

The development-restricted API inserts a `PENDING` investigation and returns immediately. A
separate worker claims it with `FOR UPDATE SKIP LOCKED`, runs the bounded LangGraph, and persists a
sanitized diagnosis and tool history. Model access is isolated behind a provider-independent
interface. The graph can invoke only registered read-only tools; it cannot issue SQL, shell
commands, unrestricted network requests, or recovery actions.

```text
API enqueue -> investigation_runs -> investigator worker -> approved tools
                                             |                  |
                                             +-> LangGraph <- sanitized PostgreSQL evidence
                                                       |
                                             deterministic citation check
                                                       |
                                             persisted bounded diagnosis
```

The historical live-provider evaluation completed one validated diagnosis in ten attempts. That
diagnosis was appropriately `INSUFFICIENT_EVIDENCE`; the sample does not establish provider
reliability or root-cause accuracy. The recovery demo therefore uses deterministic evidence
inspection and does not present itself as a live AI investigation.

## Recovery extension

Recovery is a closed, deterministic workflow over persisted plans and evidence:

1. Policy evaluates an immutable plan against incident-owned evidence, exact event identity,
   supported contracts, business invariants, and current state.
2. The verifier reloads authoritative PostgreSQL rows in a dedicated `REPEATABLE READ` transaction,
   acquires stable advisory locks, simulates only the declared write set, validates postconditions,
   and always rolls back.
3. Approval binds a trusted operator identity to the exact plan hash, verification record, evidence
   digest, state digest, policy/verifier versions, and expiry.
4. The internal executor reloads every binding under locks, re-evaluates eligibility, and commits
   the allowed business mutation with terminal audit records in one transaction.

The action catalog is limited to `REPLAY_VERIFIED_EVENT`, `RESCHEDULE_OUTBOX_EVENT`,
`RETAIN_QUARANTINE`, and `REQUEST_MANUAL_REVIEW`. Replay requires authoritative retained bytes or an
exact outbox envelope; missing data is never reconstructed. Outbox recovery changes scheduling
state only—the normal publisher remains responsible for Kafka publication.

There is no public execution endpoint or recovery worker. Runtime approval and execution
authorizers deny by default because no production identity provider is configured. Test identities
exist only inside isolated integration and demo processes.

## Benchmark and demo isolation

The benchmark runner creates a UUID-named PostgreSQL database, Redpanda topic, and consumer-group
namespace. Database work requires a persisted ownership token; broker cleanup accepts only the
run-owned namespace. Existing-resource collisions fail closed, and workers stop before groups,
topics, and the database are removed.

Reports separate `REAL_INTEGRATION`, `DETERMINISTIC_SIMULATION`, `DETERMINISTIC_FIXTURE`,
`LIVE_PROVIDER`, and `HISTORICAL` evidence. Metrics retain explicit numerator, denominator, and
unavailable counts. The artifact verifier checks checksums, schema versions, run identity, outcome
reconstruction, audit reconciliation, and sensitive-data rejection.

These controls make local results reproducible; they do not prove production availability,
multi-node durability, failover, or service-level objectives.
