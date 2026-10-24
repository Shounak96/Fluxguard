# Architecture

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
event work. They reset on restart; durable tables and logs are the audit source in this milestone.
