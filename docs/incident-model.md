# Incident model

## Lifecycle

Incidents begin in `OPEN`. `ACKNOWLEDGED` and `RESOLVED` are reserved valid states, but Milestone 2
does not expose a mutation endpoint because authentication is not yet available. Repeated evidence
updates `last_seen_at`, `updated_at`, and `occurrence_count`; the first detection time is retained.

## Failure taxonomy

| Type | Trigger | Severity | Handling |
| --- | --- | --- | --- |
| `UNSUPPORTED_SCHEMA_VERSION` | Envelope version is not v1 | MEDIUM | Quarantine |
| `MALFORMED_EVENT` | Invalid JSON or contract envelope | HIGH | Quarantine |
| `BUSINESS_INVARIANT_VIOLATION` | IDs/totals conflict with verified rules | HIGH | Quarantine |
| `POISON_MESSAGE` | Explicit development poison scenario | HIGH | Quarantine |
| `REPEATED_TRANSIENT_FAILURE` | Consumer retry threshold reached | MEDIUM | Keep retrying same offset |
| `OUTBOX_PUBLICATION_FAILURE` | Publish retry threshold reached | HIGH | Keep outbox pending |

Duplicate Kafka delivery is expected under at-least-once semantics and is not an incident.

## Correlation

The fingerprint is SHA-256 over incident type, source component, error code, and a deterministic
scope. Schema drift groups the same topic/version/scenario. Invariant failures include the affected
aggregate. Malformed messages include a raw-content hash. Controlled scenarios include their unique
scenario ID. Outbox failures include the event ID. This avoids correlating incidents solely because
their exception text happens to match.

The unique `incidents.fingerprint` constraint is the concurrency boundary. Writers use an insert-on-
conflict claim and lock the correlated row. `incident_evidence.dedup_key` prevents a retry of one
Kafka coordinate from increasing the occurrence count twice while preserving distinct records.

## Evidence and invariants

Evidence stores source, optional event ID, actual Kafka topic/partition/offset, observation time,
and a structured payload. Payloads are limited to 32 KiB, collections and strings are bounded, and
keys such as password, token, API key, customer ID, email, phone, and name are redacted.

Verified order invariants are:

- envelope aggregate ID equals payload order ID;
- order total equals the sum of line totals;
- each line total equals quantity multiplied by unit price;
- schema version and event type match the v1 contract.

Impact output reports only evidence-backed event IDs, positions, quarantine matches, and projection
state. Missing facts are labeled unavailable. Kafka lag is never translated into a record-loss
claim.

## Security limitations

Incident reads are disabled by default and only enabled when the application environment is
`development` and `INCIDENT_API_ENABLED=true`. This is a development gate, not user authentication;
do not enable it on a shared or production network. Quarantine rows retain bounded raw payload for
operators and are deliberately not returned by the incident API.
