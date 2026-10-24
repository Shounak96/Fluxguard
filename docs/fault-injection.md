# Controlled fault injection

The harness publishes named events to the real configured Redpanda broker. It is disabled unless
`ENVIRONMENT=development` and `FAULT_INJECTION_ENABLED=true`; it exits immediately in production.
Scenario payloads select fixed code paths and can never execute commands.

## Enable the development harness (PowerShell)

```powershell
$env:ENVIRONMENT = "development"
$env:FAULT_INJECTION_ENABLED = "true"
$env:INCIDENT_API_ENABLED = "true"
docker compose up --build -d --wait

$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
$env:DATABASE_URL = "postgresql+psycopg://fluxguard:fluxguard_local_only@localhost:15432/fluxguard"
```

Inject all broker scenarios in order, including a healthy event after the poison message:

```powershell
./.venv/Scripts/python.exe -m fluxguard.apps.faults all
docker compose exec -T redpanda rpk group describe fluxguard-order-projection-v1 -X brokers=localhost:9092
docker compose exec -T postgres psql -U fluxguard -d fluxguard -c "SELECT incident_id, incident_type, severity, occurrence_count, error_code FROM incidents ORDER BY detected_at DESC;"
```

Individual names are `unsupported_schema`, `malformed_event`, `invariant_violation`,
`poison_message`, `transient_processing_failure`, and `healthy_event`. Each command prints a unique
scenario ID and its expected outcome.

The outbox scenario must mark its event while the publisher is paused:

```powershell
docker compose stop outbox
./.venv/Scripts/python.exe -m fluxguard.apps.faults outbox_publish_failure
docker compose start outbox
docker compose logs --tail 100 outbox
```

It fails three attempts, records a high-severity incident at the configured threshold, and then
publishes successfully. No broker, database, volume, or unrelated container is stopped.

## Disable again

```powershell
Remove-Item Env:FAULT_INJECTION_ENABLED -ErrorAction SilentlyContinue
Remove-Item Env:INCIDENT_API_ENABLED -ErrorAction SilentlyContinue
docker compose up -d --force-recreate api outbox consumer
```

The fault switches default to false in `.env.example`. Generated scenario data is retained as audit
evidence; the harness does not delete it.
