# Milestone 4 recovery operations

Recovery moves through immutable `PROPOSED`, `VERIFIED`, `AWAITING_APPROVAL`, `APPROVED`,
`EXECUTING`, and terminal `SUCCEEDED` or `NO_OP` lifecycle events. Rejected, stale, or expired
plans fail closed. Production approval and execution remain disabled; no public execution endpoint
or unrestricted worker exists.

Retries must reuse the same execution ID and exact plan hash. A committed execution returns its
existing immutable result, so a lost caller acknowledgement cannot duplicate a projection or
outbox transition. A different request using the same execution ID is a conflict. Lock contention
or serialization failure is retryable only after reviewing its FAILED audit event and confirming
the approval is still current.

Unexpected failures roll back the business transaction before a separate sanitized FAILED audit is
written. Never retry with reconstructed event data, bypass approval, or edit audit rows. Unsupported,
malformed, poison, invariant-invalid, stale, or contradictory events remain quarantined and require
manual review. Escalate missing authoritative bytes, repeated lock conflicts, audit mismatches, or
any invariant violation rather than forcing execution.

Run the bounded local validation with a disposable migrated PostgreSQL database, local Redpanda,
isolated application workers, and `RUN_INTEGRATION=1 python -m pytest -q`. The deterministic JSON
report contracts in `recovery_safety.py` separate genuine integration coverage from simulated
process boundaries. Current production blockers are a trusted identity provider, least-privilege
database role, execution claiming/scheduling, operational monitoring, and real process-kill
durability trials.
