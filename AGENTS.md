# FluxGuard contributor instructions

FluxGuard is an invariant-aware agentic recovery platform for distributed data pipelines.

- Preserve the existing architecture and functionality. Read only task-relevant source files and documentation, and prefer focused changes over unnecessary refactoring.
- The stack is Python 3.12+, FastAPI, PostgreSQL, Redpanda, SQLAlchemy, Alembic, pytest, and Ruff. Use `.venv` for local Python execution and Docker Compose for integration testing.
- Maintain at-least-once messaging with idempotent business effects. Never advance Kafka offsets past unpersisted failures.
- LLMs may propose recovery plans, but they cannot bypass deterministic verification.
- Never claim a test passed unless it was executed. Run focused tests during implementation and full regression checks before merging.
- Never introduce paid services without explicit approval. The lifetime project budget is $10.
- Never commit `.env`, credentials, or secrets. Never delete unrelated Docker resources or volumes.
- Keep reports concise: changes, tests, blockers, and decisions.
