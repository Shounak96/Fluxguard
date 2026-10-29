# Milestone 4: invariant-verified recovery design

## Scope and safety boundary

Milestone 4 adds recovery planning, dry-run verification, approval, and audited execution. It does
not add autonomous repair. A model may propose one action from a closed catalog and cite evidence,
but deterministic application code alone decides whether the plan is eligible, verified, approved,
and executable. The executor has no arbitrary SQL, shell, URL, Kafka-offset, or payload-editing
interface.

This design reuses the existing local PostgreSQL and Redpanda Compose services. It requires no paid
service and does not change Milestone 3 investigation behavior.

## Existing components to reuse

- `parse_event` is the authoritative current-schema, envelope, aggregate, and financial-invariant
  validator. Recovery must call the same validation path rather than implement looser rules.
- `apply_event` atomically claims `processed_events.event_id` and creates at most one
  `order_projections` row. Duplicate event IDs are business no-ops; conflicting aggregate payloads
  are rejected.
- `processed_events` and `order_projections` provide unique event and projection identities.
- `quarantined_events` retains bounded original bytes and has a unique Kafka coordinate. Recovery
  never deletes or overwrites it.
- `consumer_retry_state` identifies retrying records whose Kafka offsets have not advanced.
- `outbox_events` is the authoritative source for locally produced events and already supports
  acknowledged, at-least-once publication with retry state.
- `incidents` and `incident_evidence` provide correlated fault identity, immutable evidence IDs,
  event IDs, broker coordinates, payload hashes, and occurrence history.
- `investigation_runs` may supply a grounded proposal. `INSUFFICIENT_EVIDENCE`, `UNVERIFIED`, and
  failed investigations cannot authorize a mutating action.
- Existing development gates and fault-injection scenarios remain the local test boundary.

## Recovery architecture

```text
incident/evidence/investigation
              |
              v
    proposal normalizer (closed action schema)
              |
              v
 deterministic eligibility + evidence verifier
              |
              v
 rollback-only dry run -> immutable verification result
              |
              v
 explicit approval bound to plan hash + state digest + expiry
              |
              v
 executor reacquires locks and recomputes every precondition
              |
              v
 existing parse/apply or outbox retry path -> postcondition verifier
              |
              v
 append-only execution/audit result
```

Plans and dry runs are created through trusted internal application code. The API exposes only
development-gated review and an approval route whose runtime authorizer denies by default. There is
no execution endpoint or recovery worker. Internal execution is single-action and bounded; there
is no general workflow language.

### Restricted action catalog

| Action | Effect | Eligibility | Approval |
|---|---|---|---|
| `REPLAY_VERIFIED_EVENT` | Reprocess the exact immutable event through the existing strict parser and atomic application path; never change a Kafka offset | Current schema; valid envelope and totals; trusted original bytes from a matching outbox row or retained quarantine record; no conflicting projection | Required |
| `RESCHEDULE_OUTBOX_EVENT` | Make one unpublished outbox row eligible for the existing publisher; does not publish directly | Exact incident event; `published_at IS NULL`; supported schema; envelope and payload match the authoritative target identities; current retry state captured | Required |
| `RETAIN_QUARANTINE` | Record that the event remains quarantined; no pipeline mutation | Malformed, unsupported, poison, invariant-invalid, incomplete, or otherwise unsafe evidence | Not required; auditable decision |
| `REQUEST_MANUAL_REVIEW` | Record a bounded reason and required missing evidence; no pipeline mutation | Ambiguity, stale evidence, conflicting state, unsupported repair, or policy requirement | Not required; operator owns follow-up |

`REPLAY_VERIFIED_EVENT` is intentionally narrow. It may directly invoke the same deterministic
database application primitive rather than republish to Kafka. If the normal consumer later sees
the original record, `processed_events` makes that delivery a no-op before the consumer commits its
offset. A missing or unverifiable original byte sequence is not reconstructable from a diagnosis.

Unsupported actions include editing or synthesizing payloads, translating schemas, changing
financial values, deleting processed markers/projections/quarantine, resetting or advancing Kafka
offsets, arbitrary SQL, bulk replay, incident auto-resolution, and compensation of external
systems. An unsupported schema or malformed event can only be retained or sent to manual review;
supporting it requires a separately reviewed contract and deployment, not a recovery plan.

## Enforceable invariants

1. **No duplicate event application.** The event ID is claimed once in `processed_events` in the
   same transaction as its business effect. A duplicate execution returns `NO_OP_ALREADY_APPLIED`.
2. **No duplicate order projection.** `order_projections.order_id` is primary-keyed and
   `source_event_id` is unique. An existing identical projection is a no-op; a different payload for
   the aggregate is a hard rejection.
3. **No incorrect totals.** The existing Pydantic contracts and `parse_event` recompute order and
   line totals using `Decimal`. Recovery cannot override those checks. For an outbox event, the
   verifier also binds the exact envelope and payload to the authoritative recovery target.
4. **No replay of unsupported or malformed events.** Strict decoding, schema version `1`, event
   type, required fields, aggregate identity, and all financial invariants must pass before a replay
   can be verified. Failure selects `RETAIN_QUARANTINE` or `REQUEST_MANUAL_REVIEW`.
5. **No unintended healthy-record changes.** Each action declares an exact write set. Event replay
   may insert only its processed marker and, when absent, its projection. Outbox rescheduling may
   update only the targeted unpublished outbox row's scheduling fields. Dry-run and post-execution
   snapshots must show no other row or column changes.

These guarantees cover FluxGuard's PostgreSQL effects. They do not prove absence of effects in an
unknown external consumer, which is why Milestone 4 does not publish repaired or synthesized
events.

## Plans, evidence, approval, and audit

### Immutable plan

A plan is immutable after creation and contains:

- plan ID, incident ID, action type, and policy version;
- exact target identity: event ID, aggregate ID, and broker coordinate or outbox event ID;
- canonical action parameters with a SHA-256 plan hash;
- cited evidence IDs and an evidence snapshot digest;
- expected pre-state digest; the verifier separately persists the declared write set;
- proposer kind (`OPERATOR`, `INVESTIGATION`, or `SYSTEM`) and creation time;
- verifier-policy version required to interpret it.

Edits create a new plan with a new ID and hash. Investigation text is context, not an executable
parameter. Evidence IDs must belong to the incident, and authoritative records referenced by that
evidence must still exist and match their captured hashes.

### State and audit records

Plan content stays immutable. State changes are append-only audit events:

```text
PROPOSED -> VERIFIED -> AWAITING_APPROVAL -> APPROVED -> EXECUTING
                                                  |          |
                                                  |          +-> SUCCEEDED | NO_OP | FAILED
                                                  +-> EXPIRED | REJECTED
```

Every transition records actor, timestamp, prior state, reason code, plan hash, verifier version,
and correlation ID. Verification records pre-state/evidence digests, invariant results, proposed
write set, and expiry. Approval records the exact plan and verification hashes; approving changed
or expired input is impossible. Execution attempts record an idempotency key, claimed worker,
timestamps, result, before/after digests, invariant results, and sanitized errors.

Use database uniqueness for the plan hash/idempotency key and allow at most one successful or
no-op terminal execution per plan. Audit rows are never updated or deleted; a cached current status
may be maintained transactionally for efficient claiming.

### Evidence required for mutation

A mutating plan requires all of the following:

- an existing incident and incident-owned evidence for the exact target event/coordinate;
- immutable original event bytes or an authoritative outbox envelope plus a recorded SHA-256;
- current processing state: processed marker, projection, quarantine, retry, and outbox status;
- a supported current contract and successful deterministic invariant validation;
- no conflicting projection or mismatched aggregate/event identity;
- a fresh verification result and explicit approval bound to it.

A completed, evidence-grounded investigation may propose an action but is optional. It never
replaces the evidence requirements. Missing, unrelated, stale, or contradictory evidence produces
manual review rather than a guessed repair.

## Deterministic dry run and execution

### Dry run

1. Start a PostgreSQL `REPEATABLE READ` transaction and load the plan, incident, cited evidence,
   target event, processed marker, projection, quarantine/retry state, outbox row, and authoritative
   order rows.
2. Canonicalize and hash the evidence and target pre-state. Reject missing or mismatched ownership,
   coordinates, event IDs, or payload hashes.
3. Run the existing strict parser and action-specific eligibility policy.
   The recovery policy rejects any `fault_injection` marker before parsing, regardless of the
   development fault-injection setting; recovery never interprets a test marker as healthy data.
4. Acquire the same transaction-scoped advisory lock keys the executor will use (event ID and
   aggregate ID), then simulate the exact allowed mutation with external I/O disabled.
5. Inspect the transaction's before/after state. Reject any write outside the declared rows and
   columns, and run all postcondition queries.
6. Roll back unconditionally. Persist only the sanitized verification result in a separate short
   transaction, bound to the pre-state digest and expiry.

The implementation extracts a transaction-owned event-application primitive from `apply_event`,
so dry-run and execution share code without nested transaction behavior. Unit tests may use
rollback; PostgreSQL integration tests also use a dedicated test database to prove constraints and
locking. Kafka publication is never performed during dry-run.

### Execution

The worker locks the plan, then takes transaction-scoped advisory locks for the target event and
aggregate. It reloads and rehashes every precondition. A changed digest, new evidence, expired
approval, processed event, changed projection, or changed outbox state prevents stale execution.
An already-applied identical event becomes a successful no-op; a conflicting state is rejected.

The database mutation and execution audit commit together. For `RESCHEDULE_OUTBOX_EVENT`, the
worker only changes the eligible scheduling state; the existing publisher retains responsibility
for Kafka acknowledgment and `published_at`. Post-publication success is a later observed audit
event, not falsely claimed by the scheduling transaction.

## Concurrency and replay hazards

- **Consumer/recovery race:** the consumer and executor share the atomic event-ID claim. The
  executor additionally uses target advisory locks; a concurrent consumer commit makes the
  executor no-op or fail with a typed serialization conflict. Neither deletes the processed marker.
- **Two internal executors:** the locked plan row, unique execution identity, terminal-result
  constraint, and advisory target locks prevent two business effects.
- **Missing projection row race:** locking only a nonexistent row is insufficient; use the stable
  aggregate advisory lock before checking or inserting it.
- **Outbox publisher race:** lock the outbox row using the publisher's ordering discipline. A row
  that became published makes rescheduling a no-op, never a second forced publication.
- **Stale evidence:** bind verification and approval to evidence/pre-state digests and a short
  expiry. Any new incident evidence or authoritative state change requires reverification.
- **Kafka retention or coordinate reuse assumptions:** require matching topic, partition, offset,
  event ID, and content hash. If bytes are unavailable, do not reconstruct them.
- **At-least-once delivery:** a crash after a database commit may lead to another attempt, which
  must resolve through the execution idempotency key and processed marker.
- **Incident lifecycle race:** resolving an incident is separate from recovery. Recovery success
  alone must not hide later occurrences or close a concurrently updated incident.

## Fault scenarios and acceptance tests

1. **Valid transient failure:** use an existing retry incident and exact version-1 bytes. Dry-run
   predicts one marker and one projection; execution creates each once; normal redelivery is a
   no-op and then safely commits its offset.
2. **Duplicate replay:** target an already processed event. Dry-run and repeated executions return
   `NO_OP_ALREADY_APPLIED`; projection count and payload remain unchanged.
3. **Unsafe payload matrix:** unsupported schema, malformed JSON, poison marker, aggregate mismatch,
   wrong total, and wrong line total are rejected before approval. Quarantine and healthy rows stay
   unchanged.
4. **Conflicting aggregate:** a valid event ID targeting an existing aggregate with different
   payload is rejected; the original projection and processed state are unchanged.
5. **Stale/concurrent plan:** change target state after verification and race two workers. The stale
   plan expires/rejects, only one terminal execution is recorded, and at most one business effect
   exists.
6. **Outbox reschedule:** an eligible unpublished row becomes retryable without direct publication;
   a published, unsupported, mismatched, or concurrently locked row is rejected/no-op. Existing
   publisher acknowledgment remains authoritative.

Each integration test also snapshots unrelated healthy orders, projections, processed markers,
outbox rows, and quarantine rows and asserts byte-for-byte equality after the action.

## Recommended implementation slices

1. **Contracts and policy:** closed action enums, immutable plan/verification/approval/audit
   contracts, canonical hashing, evidence ownership checks, and exhaustive policy unit tests.
2. **Persistence:** one reviewed Alembic migration for immutable plans and append-only state,
   approvals, and execution attempts; add uniqueness/check constraints and migration tests.
3. **Verifier:** shared transaction-owned apply primitive, pure eligibility checks, rollback-only
   dry run, write-set/postcondition validation, and PostgreSQL integration tests.
4. **Approval API:** development-restricted approve/read endpoints. Plan creation and dry-run remain
   trusted internal operations; approval references only a verified plan ID and hash.
5. **Executor:** internal-only execution with advisory locks, stale-state checks, idempotent
   execution, append-only audit, and safe retry behavior. No worker is enabled.
6. **Fault validation and documentation:** run the six real PostgreSQL/Redpanda scenarios in Docker,
   verify gates default off, and document actual results without live LLM dependency.

Milestone 4 acceptance requires every unsafe scenario to be rejected, every duplicate/concurrent
attempt to have at most one business effect, every dry-run to leave business state unchanged, and
every execution decision to be reproducible from immutable plan, evidence, approval, and audit
records.

### Slice 1 and 2 verification boundary

The Slice 1 policy is deliberately offline and read-only. It can validate typed evidence ownership,
content hashes, stable plan identity, supported event contracts, payload totals, target identity,
and internally consistent state snapshots.

Slice 2 persists immutable plans, normalized incident-owned evidence links, one verification
decision, and append-only lifecycle events. Real PostgreSQL tests cover idempotent and concurrent
plan creation, conflicting content, composite evidence ownership, invalid verification transitions,
transaction rollback, and database-trigger rejection of audit updates/deletes.

Slice 3 verifies the following properties with real PostgreSQL tests:

- snapshots were loaded atomically from authoritative rows at the requested isolation level;
- transaction-scoped advisory locks serialize verifiers for the same event and aggregate;
- the simulation transaction rolls back every proposed business write before audit persistence;
- a consumer/verifier race produces at most one persistent business effect;
- two verifiers create at most one immutable verification record;
- outbox row locking is compatible with the existing publisher and no Kafka I/O occurs in dry-run.

### Slice 3 rollback-only verifier

Slice 3 loads each persisted plan in a dedicated PostgreSQL `REPEATABLE READ` transaction. It
derives signed 64-bit advisory keys from SHA-256 of the event and aggregate identities, sorts the
keys numerically, and acquires them with `pg_try_advisory_xact_lock`. This gives all verifiers the
same lock order without waiting on another verifier. Outbox rows use `FOR UPDATE NOWAIT`, matching
the publisher's row-lock boundary while failing fast rather than delaying publication.

The loader accepts exact event bytes only from a matching PostgreSQL outbox row or retained
quarantine record. A broker coordinate without retained bytes is insufficient evidence. The
verifier re-runs the Slice 1 policy, simulates only the declared rows, compares complete before and
after snapshots of business tables, validates postconditions, and unconditionally rolls back.
Kafka clients are never invoked.

Only after rollback does a separate transaction append the immutable Slice 2 verification and
lifecycle records. Unexpected simulation errors and lock contention persist nothing. The Slice 5
executor therefore reloads state and re-verifies every binding because business state may change
after a dry run.

### Slice 4 review and approval boundary

Development-only review exposes sanitized plan metadata, incident-owned evidence references,
the latest verification, its expected effect and declared write set, and complete append-only
lifecycle history. `RECOVERY_API_ENABLED` defaults to false and is additionally restricted to the
development environment. It is an exposure gate, not authentication.

Approval is available only through an injected trusted authorizer. The runtime default denies all
approval because no production identity provider is configured; tests inject a deterministic
identity provider without accepting operator identity from request bodies or headers. Successful
approval appends `VERIFIED -> AWAITING_APPROVAL -> APPROVED` and an immutable approval record bound
to the exact plan hash, verification ID, evidence/state digests, policy/verifier versions, operator
subject, approval time, and expiry. A request key is stored only as a SHA-256 digest.

Before approval, PostgreSQL state and evidence are loaded again and compared with the verified
digests. Expired verification, stale state/evidence, or version drift appends `EXPIRED` and fails
closed. Identical approval requests are idempotent; a different or concurrent approval conflicts.
Approval expiry is represented by the immutable approval timestamp and by an append-only
`APPROVED -> EXPIRED` transition when observed by the approval service. No review or approval path
applies an event, publishes Kafka, or changes an outbox or other business row.

### Slice 5 controlled executor boundary

Slice 5 adds no public endpoint or worker. Trusted internal callers must supply an execution
authorizer; the only runtime implementation denies execution. Mutating actions require an exact,
unexpired persisted approval. The executor locks the plan, takes the Slice 3 advisory locks, locks
the target outbox row where applicable, reloads authoritative evidence and state, re-evaluates the
deterministic policy, and applies the already-verified write set through the shared consumer logic.

Execution requests and their STARTED/terminal events are immutable. Successful business changes,
terminal audit, and lifecycle transitions commit together. Failure audit is written separately only
after rollback and contains a sanitized error category. A repeated execution ID is idempotent;
different immutable content conflicts. PostgreSQL lock or serialization contention becomes a typed
conflict, and outbox rescheduling never publishes Kafka directly.

## Unresolved decisions

- Selection and integration of a production identity provider and authorization policy. Until then,
  runtime approval remains disabled even when development review is enabled.
- Recovery currently fails closed on policy/version drift; any future compatibility policy requires
  separate review.
- Incident resolution criteria and retention periods for recovery/audit data should remain outside
  the first execution slice unless explicitly approved.
- A dedicated least-privilege recovery database role is preferable before production use; local
  Compose can initially enforce the boundary in code and tests.
