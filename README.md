# FluxGuard

FluxGuard is an invariant-aware recovery platform for distributed data pipelines. Milestone 1
implements its reliable order-event substrate: a FastAPI producer, PostgreSQL transactional
outbox, Redpanda/Kafka publication, and an idempotent projection consumer with durable quarantine.

Milestone 2 adds deterministic incident detection, durable structured evidence, correlation,
impact analysis, and a development-only real-broker fault-injection harness. It does not use an
LLM and does not attempt autonomous recovery.

The design provides **at-least-once delivery with idempotent effects**. It deliberately does not
claim end-to-end exactly-once delivery: a crash after Kafka acknowledgment but before the outbox
update can publish the same event again.

## Components

- `api`: validates orders, calculates Decimal totals, and commits order + outbox atomically.
- `outbox`: locks pending rows, publishes with `acks=all`, then records acknowledgment.
- `consumer`: validates versioned contracts and invariants, atomically writes projection +
  processed-event marker, then commits the Kafka offset.
- PostgreSQL: authoritative orders, outbox, projections, deduplication, and quarantine.
- Redpanda: Kafka-compatible event transport on topic `orders.created.v1`.

See [architecture](docs/architecture.md), [decisions](docs/decisions.md), and
[milestones](docs/milestones.md).

## Requirements

- Python 3.12+
- Docker Desktop with Compose v2
- PowerShell 7 recommended on Windows

All Python versions are pinned in `pyproject.toml`. Container images are pinned in `compose.yaml`.
Everything used in this milestone is open source or free for local development; no API key is
needed. Values in `.env.example` are explicitly local-only credentials.

## Start locally (PowerShell)

```powershell
Copy-Item .env.example .env
docker compose up --build -d
docker compose ps
Invoke-RestMethod http://localhost:8000/health/ready
```

The API container runs `alembic upgrade head`; workers start only after API readiness.

## First complete demo

This submits an order twice with one idempotency key, observes outbox publication and projection,
republishes the exact event to demonstrate duplicate delivery, and asserts one logical effect:

```powershell
docker compose exec api python -m fluxguard.apps.demo
```

The output contains real IDs, timestamps, and database counts. Inspect each stage directly:

```powershell
docker compose exec postgres psql -U fluxguard -d fluxguard -c "SELECT id, idempotency_key, total FROM orders ORDER BY created_at DESC LIMIT 5;"
docker compose exec postgres psql -U fluxguard -d fluxguard -c "SELECT event_id, attempts, published_at, last_error FROM outbox_events ORDER BY occurred_at DESC LIMIT 5;"
docker compose exec postgres psql -U fluxguard -d fluxguard -c "SELECT order_id, source_event_id, total FROM order_projections ORDER BY projected_at DESC LIMIT 5;"
docker compose exec postgres psql -U fluxguard -d fluxguard -c "SELECT event_id, processed_at, last_seen_at, delivery_count FROM processed_events ORDER BY processed_at DESC LIMIT 5;"
docker compose logs --tail 100 outbox consumer
```

Manual API request and replay:

```powershell
$headers = @{ "Idempotency-Key" = "demo-order-001"; "Content-Type" = "application/json" }
$body = '{"customer_id":"cust_123","items":[{"product_id":"prod_101","quantity":2,"unit_price":"24.99"}],"currency":"USD"}'
$first = Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/v1/orders -Headers $headers -Body $body
$second = Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/v1/orders -Headers $headers -Body $body
$first | ConvertTo-Json -Depth 8
$second | ConvertTo-Json -Depth 8
```

## Development and tests

```powershell
python -m pip install -e ".[dev]"
python -m ruff format --check .
python -m ruff check .
python -m pytest -q -m "not integration"
```

With the Compose dependencies running, execute the real integration test from the host:

```powershell
$env:RUN_INTEGRATION = "1"
$env:DATABASE_URL = "postgresql+psycopg://fluxguard:fluxguard_local_only@localhost:15432/fluxguard"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
python -m pytest -q tests/integration
```

The equivalent task shortcuts are `./scripts/dev.ps1 -Task up`, `test`, `lint`, `demo`, and
`integration`. Stop without deleting durable data using `docker compose down`. To intentionally
delete only this project's Compose volumes, use `docker compose down --volumes`.

## Endpoints

- `POST /api/v1/orders` — requires `Idempotency-Key`.
- `GET /health/live` — process liveness.
- `GET /health/ready` — database readiness.
- `GET /metrics` — process-local basic counters.
- `GET /docs` — OpenAPI UI.

Development-only incident reads are disabled by default. With `ENVIRONMENT=development` and
`INCIDENT_API_ENABLED=true`, these endpoints are available:

- `GET /api/v1/incidents`
- `GET /api/v1/incidents/{incident_id}`
- `GET /api/v1/incidents/{incident_id}/evidence`
- `GET /api/v1/incidents/{incident_id}/impact`

Structured logs include correlation IDs on API requests and event IDs during event work.
See [incident model](docs/incident-model.md) and
[fault injection](docs/fault-injection.md) for Milestone 2 operations.
