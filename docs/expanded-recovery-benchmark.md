# Expanded recovery reliability benchmark

This document records one bounded local experiment that extended FluxGuard's existing isolated
recovery-safety runner. It did not change recovery policy, verification, approval, or execution
behavior.

## Observed run

| Field | Value |
| --- | --- |
| Run ID | `60e9ccf1-de1f-4e9e-804a-e709fefcf723` |
| Seed | `20261117` |
| Mode | `EXPANDED_RECOVERY_SAFETY` |
| Evidence class | `REAL_INTEGRATION` only |
| PostgreSQL migration | `0006_recovery_execution` |
| Python | 3.12.16 |
| Total run duration | 38,819.426 ms |
| Attempt records | 120 |
| Scenario families | 4 |
| Distinct variations | 13 |

The runner created a UUID-named disposable PostgreSQL database, run-scoped Redpanda topic and
consumer groups, and deterministic event identities. Every attempt received a derived and recorded
trial seed. All database and broker resources were removed afterward; the development database
counts matched their pre-run baseline.

## Methodology and denominators

The command below reproduces the configuration, not the exact observed timing or run ID:

```powershell
docker compose run --rm --no-deps `
  -v "${PWD}:/workspace" -w /workspace `
  -e BENCHMARK_POSTGRES_ADMIN_URL=postgresql+psycopg://fluxguard:fluxguard_local_only@postgres:5432/postgres `
  -e BENCHMARK_KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 `
  api python -m fluxguard.apps.benchmark `
    --mode expanded-recovery-safety `
    --family-trials 30 `
    --concurrency-trials 10 `
    --seed 20261117 `
    --output-dir build/expanded-recovery/full
```

The 120 attempts were partitioned before execution:

| Family | Attempts | Variations |
| --- | ---: | ---: |
| Eligible recovery | 30 | 2 |
| Unsafe-action rejection | 30 | 7 |
| Duplicate/no-op correctness | 30 | 1 |
| Concurrency safety | 30 | 3 |

Eligible recovery alternated verified event replay and outbox rescheduling. Unsafe cases covered
unsupported schema, malformed event, invalid total, missing evidence, foreign evidence, stale
approval, and expired approval. Concurrency used ten trials each for executor/executor,
consumer/executor, and publisher/executor races.

These are repeated bounded trials across 13 variations, not 120 statistically independent fault
categories. Concurrency was coordinated with in-process threads over real PostgreSQL transactions
and Redpanda access; it was not a multi-host or process-kill stress test.

## Actual outcomes

| Measurement | Result | Denominator |
| --- | ---: | --- |
| Eligible recoveries | 30/30 (100%) | Attempts in the eligible-recovery family |
| Unsafe actions rejected | 30/30 (100%) | Attempts in the unsafe-rejection family |
| Verified no-ops | 30/30 (100%) | Attempts in the duplicate/no-op family |
| Concurrency-safe outcomes | 30/30 (100%) | Ten trials for each of three race variations |
| Expected outcomes matched | 120/120 (100%) | All recorded attempts |
| Invariant violations | 0/120 (0%) | All recorded attempts |
| Duplicate business effects | 0/120 (0%) | All recorded attempts |
| Unexpected Kafka publications | 0/120 (0%) | All recorded attempts |

Actual terminal classifications were 50 `RECOVERED`, 30 `SAFELY_REJECTED`, 30
`VERIFIED_NO_OP`, and 10 `OPERATIONAL_FAILURE`. All ten operational failures were expected
`CONCURRENT_CONSUMER_WON` outcomes: the consumer committed the sole business effect while the
executor failed closed. There were no unexpected operational failures and no discarded attempts.

Independent assertions recorded valid declared write sets, processed-event cardinality, projection
cardinality, outbox effects, and preserved invariants for all 120 attempts.

## Audit reconciliation

Scenario-level audit assertions passed 120/120. Immutable lifecycle reconciliation was more
specific:

- 111/120 traces reconciled (`CONSISTENT`)
- 9/120 were `UNAVAILABLE`
- 0/120 were inconsistent

The unavailable set comprised four missing-evidence and five foreign-evidence attempts that failed
before a verification record existed. They were safe rejections, but they must not be described as
reconciled immutable audit traces.

## Latency coverage

Latency values are per-scenario monotonic measurements from this run:

| Stage | Samples | Unavailable | Median | p95 |
| --- | ---: | ---: | ---: | ---: |
| End to end | 120 | 0 | 220.310 ms | 1,184.487 ms |
| Execution | 94 | 26 | 61.564 ms | 135.791 ms |
| Verification | 111 | 9 | 47.213 ms | 80.960 ms |
| Plan creation | 81 | 39 | 22.225 ms | 30.725 ms |
| Approval | 99 | 21 | 15.379 ms | 20.723 ms |

No samples were recorded for API submission, outbox publication, Kafka watermark timing, consumer
processing, fault injection, incident persistence, investigation, deterministic inspection, or
final-state validation. The runner did perform broker watermark checks and independent final-state
assertions, but their durations were not separately instrumented. The execution and end-to-end
sample sets differ, so their latency difference is not a causal stage attribution.

## Artifacts and schema compatibility

The local bundle contains a schema-v2 benchmark report, schema-v1 operational summary, and
schema-v1 checksum manifest. It can be checked without executing another benchmark:

```powershell
.\.venv\Scripts\python.exe -m fluxguard.apps.verify_benchmark `
  build/expanded-recovery/full/60e9ccf1-de1f-4e9e-804a-e709fefcf723 `
  --format json
```

Schema-v2 identity fields `scenario_family`, `variation`, and `trial_seed`, plus report-level
`scenario_groups`, are optional additions. Current readers accept earlier v2 reports with those
fields absent, and the existing observer and artifact verifier continue to operate on both shapes.
Because contracts reject unknown fields, pre-extension reader binaries are not forward-compatible
with expanded reports and must be upgraded. This is reader-backward compatibility, not universal
bidirectional compatibility.

The generated JSON bundle is intentionally not committed. The report and summary were independently
recalculated from individual attempt records, and manifest file sizes and SHA-256 checksums matched.

## Provenance and limitations

The run recorded its run ID, seed, Python version, migration revision, timestamps, and run-owned
resource names. Git SHA, dirty-worktree state, Docker/Compose versions, and PostgreSQL/Redpanda image
identifiers were unavailable and are not inferred here.

This was a small local, single-node experiment. It does not demonstrate production availability,
multi-node consensus behavior, process-crash durability, root-cause accuracy, or a service-level
objective. Production recovery authorization and execution remain disabled, and no LLM or paid
provider was used.
