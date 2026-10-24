param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("install", "lint", "test", "up", "down", "logs", "demo", "integration")]
    [string]$Task
)

$ErrorActionPreference = "Stop"

switch ($Task) {
    "install" { python -m pip install -e ".[dev]" }
    "lint" {
        python -m ruff format --check .
        python -m ruff check .
    }
    "test" { python -m pytest -q -m "not integration" }
    "up" {
        if (-not (Test-Path -LiteralPath ".env")) { Copy-Item ".env.example" ".env" }
        docker compose up --build -d
        docker compose ps
    }
    "down" { docker compose down }
    "logs" { docker compose logs -f api outbox consumer }
    "demo" { docker compose exec api python -m fluxguard.apps.demo }
    "integration" {
        $env:RUN_INTEGRATION = "1"
        $env:DATABASE_URL = "postgresql+psycopg://fluxguard:fluxguard_local_only@localhost:15432/fluxguard"
        $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
        python -m pytest -q tests/integration
    }
}
