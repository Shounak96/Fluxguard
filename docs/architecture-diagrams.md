# Architecture and trust-boundary diagrams

These diagrams describe the implemented local architecture. They separate optional AI diagnosis
from deterministic recovery authority and do not imply production recovery availability.

## Component architecture

```mermaid
flowchart LR
    Client[API client]

    subgraph Processes[Application processes]
        API[FastAPI]
        Publisher[Outbox publisher]
        Consumer[Idempotent consumer]
        Investigator[Bounded investigator]
    end

    Broker[Redpanda]

    subgraph Postgres[PostgreSQL]
        Orders[Orders and outbox]
        Projections[Projections and processed markers]
        Failures[Quarantine, incidents, and evidence]
        Investigations[Investigation runs]
        RecoveryStore[Plans, verification, approval, execution, and audit]
    end

    subgraph Recovery[Deterministic recovery control]
        Tools[Approved read-only tools]
        Policy[Recovery planner and policy]
        Verifier[Rollback-only verifier]
        Approval[Trusted approval boundary]
        Executor[Internal restricted executor]
    end

    Client -->|Idempotent order| API
    API -->|One transaction| Orders
    Orders -->|Lock pending outbox row| Publisher
    Publisher -->|acks=all| Broker
    Broker -->|Versioned event| Consumer
    Consumer -->|Valid event transaction| Projections
    Consumer -->|Permanent failure transaction| Failures
    Publisher -->|Retry-threshold evidence| Failures

    Investigator -->|Invoke typed tools| Tools
    Tools -->|Sanitized reads| Failures
    Tools -->|Read processing state| Projections
    Investigator -->|Persist bounded diagnosis| Investigations
    Investigations -.->|Optional context only| Policy

    Failures -->|Incident-owned evidence| Policy
    Orders -->|Authoritative target state| Policy
    Policy -->|Persist immutable plan| RecoveryStore
    Policy --> Verifier
    Verifier -->|Reload and simulate| Orders
    Verifier -->|Reload and simulate| Projections
    Verifier -->|Persist decision| RecoveryStore
    RecoveryStore --> Approval
    Approval -->|Exact unexpired binding| Executor
    Executor -->|Declared write set only| Orders
    Executor -->|Replay effect or verified no-op| Projections
    Executor -->|Append terminal records| RecoveryStore

    Orders -.->|Rescheduled row uses normal path| Publisher
```

The executor never publishes directly to Redpanda. `RESCHEDULE_OUTBOX_EVENT` changes only eligible
outbox scheduling fields; the ordinary publisher remains responsible for broker acknowledgement.

## Trust boundaries

```mermaid
flowchart TB
    subgraph Untrusted[Untrusted inputs]
        Commands[External API commands]
        Events[External event bytes]
        ModelOutput[LLM output]
    end

    subgraph Validation[Validated pipeline boundary]
        APIValidation[Command validation and request hash]
        EventValidation[Schema, envelope, and invariant validation]
        IncidentEvidence[Sanitized incident evidence]
    end

    subgraph AI[Read-only AI boundary]
        Agent[Bounded investigation graph]
        ReadTools[Closed read-only tool registry]
        Diagnosis[Validated diagnosis and citations]
        NoAuthority[No mutation or authorization authority]
    end

    subgraph Deterministic[Deterministic safety boundary]
        Policy[Versioned recovery policy]
        Verifier[Authoritative PostgreSQL dry run]
    end

    subgraph Authorization[Trusted approval boundary]
        RuntimeDeny[Deny by default in runtime]
        Denied[No approval created]
        TestIdentity[Test identity in isolated fixtures only]
        BoundApproval[Plan and verification-bound approval]
    end

    subgraph Mutation[Internal mutation boundary]
        Executor[Restricted internal executor]
    end

    subgraph Authority[Authoritative PostgreSQL state]
        Business[Business and processing state]
        Audit[Immutable audit history]
    end

    Commands --> APIValidation --> Business
    Events --> EventValidation
    EventValidation -->|Valid| Business
    EventValidation -->|Invalid| IncidentEvidence
    IncidentEvidence --> ReadTools
    Business -->|Bounded reads| ReadTools
    ModelOutput --> Agent
    Agent --> ReadTools --> Diagnosis
    Diagnosis --> NoAuthority
    Diagnosis -.->|Evidence-cited context, never authorization| Policy

    IncidentEvidence --> Policy
    Business --> Policy
    Policy --> Verifier
    Verifier -->|Repeatable-read simulation and rollback| Business
    Verifier --> Audit

    RuntimeDeny -->|Reject runtime request| Denied
    TestIdentity -->|Demo and tests only| BoundApproval
    Verifier -->|Eligible current verification| BoundApproval
    BoundApproval --> Executor
    Executor -->|Recheck and declared writes only| Business
    Executor --> Audit
```

Model output is untrusted input. Pydantic validation and citation checks can make a diagnosis
well-formed and evidence-grounded, but only deterministic policy, fresh PostgreSQL verification,
and trusted approval can authorize a mutating action. The shipped runtime authorizers deny approval
and execution because no production identity provider is configured.

## Recovery sequence

This sequence follows the hosted demo's eligible outbox-rescheduling lane. The demo uses
deterministic evidence inspection and makes no LLM request.

```mermaid
sequenceDiagram
    autonumber
    participant DB as PostgreSQL state
    participant Inspect as Evidence inspection
    participant Policy as Recovery policy
    participant Verify as Rollback-only verifier
    participant Auth as Approval boundary
    participant Execute as Internal executor
    participant Audit as Immutable audit tables
    participant Publish as Ordinary publisher
    participant Kafka as Redpanda
    participant Consume as Ordinary consumer
    participant Reconcile as Audit reconciliation

    DB->>DB: Persist outbox failure incident and owned evidence
    Inspect->>DB: Read incident, evidence, and exact outbox state
    Inspect->>Policy: Supply authoritative evidence contracts
    Policy->>DB: Persist immutable deterministic plan
    Policy->>Verify: Request dry-run verification
    Verify->>DB: Begin REPEATABLE READ and reload fresh state
    Verify->>DB: Acquire locks and simulate declared write set
    Verify->>DB: Validate invariants and unrelated-state equality
    Verify->>DB: Roll back simulation unconditionally

    alt Unsafe or insufficient evidence
        Verify->>Audit: Append rejection or insufficient-evidence decision
        Note over Auth,Execute: No approval, execution, or Kafka publication
    else Eligible outbox reschedule
        Verify->>Audit: Persist eligible verification and state digest
        Auth-->>Auth: Default runtime authorizer denies request
        Note over Auth: Production approval remains disabled
        Auth->>DB: Test-only identity approves exact verification
        Auth->>Audit: Append bound approval and expiry
        Execute->>DB: Lock plan and target, then reload all bindings
        Execute->>DB: Re-evaluate policy and reschedule target outbox row
        Execute->>Audit: Commit business write and terminal audit atomically
        Note over Execute,Kafka: Executor performs no direct publication
        Publish->>DB: Lock now-eligible outbox row
        Publish->>Kafka: Publish through normal path
        Kafka-->>Publish: Broker acknowledgement
        Publish->>DB: Mark outbox row published
        Kafka->>Consume: Deliver versioned event
        Consume->>DB: Commit projection and processed marker atomically
        Consume->>Kafka: Commit offset after PostgreSQL commit
        Inspect->>DB: Validate final state, cardinality, and write set
        Reconcile->>Audit: Read immutable lifecycle references
        Reconcile->>DB: Compare audit outcome with final business state
    end
```

For `REPLAY_VERIFIED_EVENT`, the executor uses the shared consumer application logic with exact
authoritative bytes. Unsupported, malformed, poison, invariant-invalid, stale, or foreign-evidence
plans take the rejection branch and cannot reach approval or mutation.
