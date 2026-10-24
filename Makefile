.PHONY: install lint test up down logs demo integration

install:
	python -m pip install -e ".[dev]"
lint:
	python -m ruff format --check .
	python -m ruff check .
test:
	python -m pytest -q -m "not integration"
up:
	docker compose up --build -d
down:
	docker compose down
logs:
	docker compose logs -f api outbox consumer
demo:
	docker compose exec api python -m fluxguard.apps.demo
integration:
	RUN_INTEGRATION=1 python -m pytest -q tests/integration
