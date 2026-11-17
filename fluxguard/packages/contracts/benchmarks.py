import re
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

_SENSITIVE_TEXT = re.compile(
    r"(?:postgres(?:ql)?|mysql|mongodb|redis|https?)://"
    r"|(?:api[_-]?key|password|secret|token)\s*[:=]\s*\S+"
    r"|bearer\s+\S+"
    r"|-----begin(?: [a-z]+)? private key-----"
    r"|\bsk-[a-z0-9_-]{16,}",
    re.IGNORECASE,
)


def _safe_text(value: str) -> str:
    if _SENSITIVE_TEXT.search(value):
        raise ValueError("sensitive values and connection strings are not permitted")
    return value


class FrozenContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EvidenceClass(StrEnum):
    REAL_INTEGRATION = "REAL_INTEGRATION"
    DETERMINISTIC_SIMULATION = "DETERMINISTIC_SIMULATION"
    DETERMINISTIC_FIXTURE = "DETERMINISTIC_FIXTURE"
    LIVE_PROVIDER = "LIVE_PROVIDER"
    HISTORICAL = "HISTORICAL"


class BenchmarkOutcome(StrEnum):
    RECOVERED = "RECOVERED"
    VERIFIED_NO_OP = "VERIFIED_NO_OP"
    SAFELY_REJECTED = "SAFELY_REJECTED"
    OPERATIONAL_FAILURE = "OPERATIONAL_FAILURE"


class BenchmarkStage(StrEnum):
    API_ORDER_SUBMISSION = "API_ORDER_SUBMISSION"
    OUTBOX_PUBLICATION = "OUTBOX_PUBLICATION"
    KAFKA_WATERMARK = "KAFKA_WATERMARK"
    CONSUMER_PROCESSING = "CONSUMER_PROCESSING"
    FAULT_INJECTION = "FAULT_INJECTION"
    INCIDENT_PERSISTENCE = "INCIDENT_PERSISTENCE"
    INVESTIGATION = "INVESTIGATION"
    DETERMINISTIC_INSPECTION = "DETERMINISTIC_INSPECTION"
    PLAN_CREATION = "PLAN_CREATION"
    VERIFICATION = "VERIFICATION"
    APPROVAL = "APPROVAL"
    EXECUTION = "EXECUTION"
    FINAL_STATE_VALIDATION = "FINAL_STATE_VALIDATION"
    END_TO_END = "END_TO_END"


class BenchmarkMode(StrEnum):
    ISOLATION_SMOKE = "ISOLATION_SMOKE"
    RECOVERY_SAFETY = "RECOVERY_SAFETY"
    END_TO_END_DEMO = "END_TO_END_DEMO"


class OperationalDisposition(StrEnum):
    RECOVERY_EXECUTION_SUCCESS = "RECOVERY_EXECUTION_SUCCESS"
    VERIFIED_NO_OP = "VERIFIED_NO_OP"
    SAFE_REJECTION = "SAFE_REJECTION"
    EXPECTED_OPERATIONAL_CONFLICT = "EXPECTED_OPERATIONAL_CONFLICT"
    EXPECTED_OPERATIONAL_FAILURE = "EXPECTED_OPERATIONAL_FAILURE"
    UNEXPECTED_OPERATIONAL_FAILURE = "UNEXPECTED_OPERATIONAL_FAILURE"
    SAFETY_ASSERTION_FAILURE = "SAFETY_ASSERTION_FAILURE"


class AuditReconciliationStatus(StrEnum):
    CONSISTENT = "CONSISTENT"
    INCONSISTENT = "INCONSISTENT"
    UNAVAILABLE = "UNAVAILABLE"


class ObservableLifecycleState(StrEnum):
    PROPOSED = "PROPOSED"
    VERIFIED = "VERIFIED"
    REJECTED = "REJECTED"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    APPROVED = "APPROVED"
    EXECUTING = "EXECUTING"
    EXECUTED = "EXECUTED"
    NO_OP = "NO_OP"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


class BenchmarkResourceNames(FrozenContract):
    database_name: str = Field(pattern=r"^fluxguard_benchmark_[0-9a-f]{32}$")
    kafka_topic: str = Field(pattern=r"^fluxguard-benchmark-[0-9a-f]{32}-orders$")
    kafka_consumer_group: str = Field(pattern=r"^fluxguard-benchmark-[0-9a-f]{32}-consumer$")


class BenchmarkConfiguration(FrozenContract):
    benchmark_name: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9][a-z0-9_-]*$")
    mode: BenchmarkMode
    seed: int = Field(ge=0)
    serial_trials: int = Field(ge=1, le=100)
    concurrency_trials: int = Field(ge=1, le=100)
    provider_calls_enabled: bool = False
    resources: BenchmarkResourceNames


class BenchmarkRunMetadata(FrozenContract):
    run_id: UUID
    started_at: datetime
    completed_at: datetime
    duration_ms: float = Field(ge=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_timestamps(self) -> "BenchmarkRunMetadata":
        if self.started_at.tzinfo is None or self.completed_at.tzinfo is None:
            raise ValueError("benchmark timestamps must be timezone-aware")
        if self.started_at.astimezone(UTC) > self.completed_at.astimezone(UTC):
            raise ValueError("benchmark completion cannot precede its start")
        return self


class BenchmarkEnvironment(FrozenContract):
    git_sha: str | None = Field(default=None, pattern=r"^[0-9a-f]{40}$")
    dirty_worktree: bool | None = None
    python_version: str | None = Field(default=None, min_length=1, max_length=100)
    docker_version: str | None = Field(default=None, min_length=1, max_length=100)
    compose_version: str | None = Field(default=None, min_length=1, max_length=100)
    postgres_image: str | None = Field(default=None, min_length=1, max_length=255)
    redpanda_image: str | None = Field(default=None, min_length=1, max_length=255)
    migration_revision: str | None = Field(default=None, min_length=1, max_length=100)

    @model_validator(mode="after")
    def reject_sensitive_values(self) -> "BenchmarkEnvironment":
        for name in (
            "python_version",
            "docker_version",
            "compose_version",
            "postgres_image",
            "redpanda_image",
            "migration_revision",
        ):
            value = getattr(self, name)
            if value is not None:
                _safe_text(value)
        return self


class ScenarioIdentity(FrozenContract):
    scenario_id: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9][a-z0-9_-]*$")
    trial: int = Field(ge=1)
    evidence_class: EvidenceClass
    expected_outcome: BenchmarkOutcome
    concurrency: bool = False
    required: bool = True


class AssertionResults(FrozenContract):
    invariants_preserved: bool | None = None
    declared_write_set_only: bool | None = None
    processed_event_cardinality_valid: bool | None = None
    projection_cardinality_valid: bool | None = None
    outbox_effect_valid: bool | None = None
    duplicate_business_effects: int | None = Field(default=None, ge=0)
    unexpected_kafka_publications: int | None = Field(default=None, ge=0)
    audit_consistent: bool | None = None
    concurrency_safe: bool | None = None
    business_mutation_applied: bool | None = None


class StageTiming(FrozenContract):
    stage: BenchmarkStage
    duration_ms: float = Field(ge=0, allow_inf_nan=False)


class ResourceUsage(FrozenContract):
    model_id: str | None = Field(default=None, min_length=1, max_length=255)
    llm_calls: int | None = Field(default=None, ge=0)
    prompt_tokens: int | None = Field(default=None, ge=0)
    completion_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    provider_cost_usd: Decimal | None = Field(default=None, ge=0, max_digits=12, decimal_places=6)

    @model_validator(mode="after")
    def validate_usage(self) -> "ResourceUsage":
        if self.model_id is not None:
            _safe_text(self.model_id)
        if (
            self.prompt_tokens is not None
            and self.completion_tokens is not None
            and self.total_tokens is not None
            and self.prompt_tokens + self.completion_tokens != self.total_tokens
        ):
            raise ValueError("total_tokens must equal prompt_tokens plus completion_tokens")
        if self.llm_calls == 0 and any(
            value not in (None, 0)
            for value in (self.prompt_tokens, self.completion_tokens, self.total_tokens)
        ):
            raise ValueError("zero LLM calls cannot report positive token usage")
        return self


class SanitizedError(FrozenContract):
    category: str = Field(min_length=1, max_length=100, pattern=r"^[A-Z][A-Z0-9_]*$")
    stage: BenchmarkStage
    retryable: bool
    message: str | None = Field(default=None, min_length=1, max_length=500)

    @model_validator(mode="after")
    def reject_sensitive_message(self) -> "SanitizedError":
        if self.message is not None:
            _safe_text(self.message)
        return self


class AuditLifecycleReference(FrozenContract):
    event_id: UUID
    sequence_number: int = Field(ge=1)
    state: ObservableLifecycleState
    reason_code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,99}$")


class RecoveryAuditTrace(FrozenContract):
    incident_id: UUID
    plan_id: UUID | None = None
    verification_id: UUID | None = None
    approval_id: UUID | None = None
    execution_id: UUID | None = None
    lifecycle: tuple[AuditLifecycleReference, ...] = Field(default_factory=tuple)
    safe_reason_codes: tuple[str, ...] = Field(default_factory=tuple)
    verification_reason_codes: tuple[str, ...] = Field(default_factory=tuple)
    execution_reason_codes: tuple[str, ...] = Field(default_factory=tuple)
    reconciliation_status: AuditReconciliationStatus
    reconciliation_issues: tuple[str, ...] = Field(default_factory=tuple)

    @model_validator(mode="after")
    def validate_trace(self) -> "RecoveryAuditTrace":
        code_pattern = re.compile(r"^[A-Z][A-Z0-9_]{0,99}$")
        codes = (
            *self.safe_reason_codes,
            *self.verification_reason_codes,
            *self.execution_reason_codes,
            *self.reconciliation_issues,
        )
        if any(code_pattern.fullmatch(code) is None for code in codes):
            raise ValueError("audit trace reason and issue codes must be sanitized identifiers")
        sequences = [event.sequence_number for event in self.lifecycle]
        if sequences != sorted(sequences) or len(sequences) != len(set(sequences)):
            raise ValueError("audit lifecycle references must have unique ordered sequences")
        if self.plan_id is None:
            if (
                any(
                    value is not None
                    for value in (self.verification_id, self.approval_id, self.execution_id)
                )
                or self.lifecycle
            ):
                raise ValueError("audit references cannot exist without a persisted plan")
            if self.reconciliation_status != AuditReconciliationStatus.UNAVAILABLE:
                raise ValueError("an unpersisted plan has unavailable audit reconciliation")
        if (
            self.reconciliation_status == AuditReconciliationStatus.CONSISTENT
            and self.reconciliation_issues
        ):
            raise ValueError("consistent audit traces cannot contain reconciliation issues")
        return self


class ScenarioResult(FrozenContract):
    identity: ScenarioIdentity
    actual_outcome: BenchmarkOutcome
    assertions: AssertionResults
    timings: tuple[StageTiming, ...] = Field(default_factory=tuple, max_length=20)
    usage: ResourceUsage | None = None
    error: SanitizedError | None = None
    audit_trace: RecoveryAuditTrace | None = None
    limitations: tuple[str, ...] = Field(default_factory=tuple, max_length=20)

    @model_validator(mode="after")
    def validate_consistency(self) -> "ScenarioResult":
        stages = [item.stage for item in self.timings]
        if len(stages) != len(set(stages)):
            raise ValueError("stage timings must contain at most one value per stage")
        for limitation in self.limitations:
            if not limitation or len(limitation) > 500:
                raise ValueError("limitations must contain between 1 and 500 characters")
            _safe_text(limitation)

        if self.actual_outcome == BenchmarkOutcome.OPERATIONAL_FAILURE:
            if self.error is None:
                raise ValueError("operational failures require a sanitized error")
            return self
        if self.error is not None:
            raise ValueError("only operational failures may include an error")

        assertion_values = (
            self.assertions.invariants_preserved,
            self.assertions.declared_write_set_only,
            self.assertions.processed_event_cardinality_valid,
            self.assertions.projection_cardinality_valid,
            self.assertions.outbox_effect_valid,
            self.assertions.duplicate_business_effects,
            self.assertions.unexpected_kafka_publications,
            self.assertions.audit_consistent,
            self.assertions.business_mutation_applied,
        )
        if any(value is None for value in assertion_values):
            raise ValueError("non-operational outcomes require complete safety assertions")
        if not self.assertions.invariants_preserved:
            raise ValueError("a terminal safety outcome cannot claim an invariant violation")
        if not self.assertions.declared_write_set_only:
            raise ValueError("a terminal safety outcome cannot claim an undeclared write")
        if not self.assertions.processed_event_cardinality_valid:
            raise ValueError("a terminal safety outcome requires valid processed-event cardinality")
        if not self.assertions.projection_cardinality_valid:
            raise ValueError("a terminal safety outcome requires valid projection cardinality")
        if not self.assertions.outbox_effect_valid:
            raise ValueError("a terminal safety outcome requires a valid outbox effect")
        if self.assertions.duplicate_business_effects != 0:
            raise ValueError("a terminal safety outcome cannot claim duplicate business effects")
        if self.assertions.unexpected_kafka_publications != 0:
            raise ValueError("a terminal safety outcome cannot claim an unexpected publication")
        if not self.assertions.audit_consistent:
            raise ValueError("a terminal safety outcome requires consistent audit state")
        if self.identity.concurrency and self.assertions.concurrency_safe is not True:
            raise ValueError("a conclusive concurrency outcome must be concurrency-safe")
        expected_mutation = self.actual_outcome == BenchmarkOutcome.RECOVERED
        if self.assertions.business_mutation_applied is not expected_mutation:
            raise ValueError("business mutation assertion contradicts the actual outcome")
        return self


class MetricRate(FrozenContract):
    numerator: int = Field(ge=0)
    denominator: int = Field(ge=0)
    unavailable: int = Field(ge=0)
    rate_percent: float | None = Field(default=None, ge=0, le=100, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_rate(self) -> "MetricRate":
        if self.numerator > self.denominator:
            raise ValueError("metric numerator cannot exceed denominator")
        expected = (
            None if self.denominator == 0 else round(100 * self.numerator / self.denominator, 1)
        )
        if self.rate_percent != expected:
            raise ValueError("metric rate does not match its numerator and denominator")
        return self


class BenchmarkMetrics(FrozenContract):
    safe_recovery_success: MetricRate
    verified_no_op_correctness: MetricRate
    unsafe_action_rejection: MetricRate
    duplicate_business_effects: MetricRate
    invariant_violations: MetricRate
    audit_consistency: MetricRate
    concurrency_safety: MetricRate
    operational_completion: MetricRate


class StageLatencySummary(FrozenContract):
    stage: BenchmarkStage
    count: int = Field(ge=0)
    unavailable: int = Field(ge=0)
    min_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    median_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    p95_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    max_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_summary(self) -> "StageLatencySummary":
        values = (self.min_ms, self.median_ms, self.p95_ms, self.max_ms)
        if self.count == 0 and any(value is not None for value in values):
            raise ValueError("empty latency summaries cannot contain fabricated values")
        if self.count > 0 and any(value is None for value in values):
            raise ValueError("non-empty latency summaries require all summary values")
        if self.count > 0 and not (
            self.min_ms <= self.median_ms <= self.max_ms
            and self.min_ms <= self.p95_ms <= self.max_ms
        ):
            raise ValueError("latency summary values are inconsistent")
        return self


class EvidenceClassSummary(FrozenContract):
    evidence_class: EvidenceClass
    trials: int = Field(ge=0)
    metrics: BenchmarkMetrics
    latency: tuple[StageLatencySummary, ...]
    limitations: tuple[str, ...] = Field(default_factory=tuple)


class BenchmarkReport(FrozenContract):
    schema_version: Literal[2] = 2
    run: BenchmarkRunMetadata
    configuration: BenchmarkConfiguration
    environment: BenchmarkEnvironment
    results: tuple[ScenarioResult, ...]
    cohorts: tuple[EvidenceClassSummary, ...]
    limitations: tuple[str, ...] = Field(default_factory=tuple, max_length=100)

    @model_validator(mode="after")
    def validate_report_shape(self) -> "BenchmarkReport":
        run_token = self.run.run_id.hex
        resources = self.configuration.resources
        if any(
            run_token not in value
            for value in (
                resources.database_name,
                resources.kafka_topic,
                resources.kafka_consumer_group,
            )
        ):
            raise ValueError("resource names must be derived from the benchmark run ID")
        identities = [
            (item.identity.evidence_class, item.identity.scenario_id, item.identity.trial)
            for item in self.results
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("scenario identity and trial must be unique within an evidence class")
        cohort_classes = [item.evidence_class for item in self.cohorts]
        if cohort_classes != list(EvidenceClass):
            raise ValueError("report must contain one ordered cohort for every evidence class")
        counts = {
            evidence_class: sum(
                result.identity.evidence_class == evidence_class for result in self.results
            )
            for evidence_class in EvidenceClass
        }
        if any(cohort.trials != counts[cohort.evidence_class] for cohort in self.cohorts):
            raise ValueError("cohort trial counts do not match report results")
        for limitation in self.limitations:
            if not limitation or len(limitation) > 500:
                raise ValueError("limitations must contain between 1 and 500 characters")
            _safe_text(limitation)
        return self


class CodeCount(FrozenContract):
    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,99}$")
    count: int = Field(ge=1)


class MissingTelemetryCounts(FrozenContract):
    audit_trace: int = Field(ge=0)
    incident_id: int = Field(ge=0)
    plan_id: int = Field(ge=0)
    verification_id: int = Field(ge=0)
    approval_id: int = Field(ge=0)
    execution_id: int = Field(ge=0)


class OperationalOutcomeCounts(FrozenContract):
    recovery_execution_success: int = Field(ge=0)
    verified_no_op: int = Field(ge=0)
    safe_rejection: int = Field(ge=0)
    expected_operational_conflict: int = Field(ge=0)
    expected_operational_failure: int = Field(ge=0)
    unexpected_operational_failure: int = Field(ge=0)
    safety_assertion_failure: int = Field(ge=0)


class ScenarioOperationalTrace(FrozenContract):
    run_id: UUID
    scenario_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")
    trial: int = Field(ge=1)
    evidence_class: EvidenceClass
    disposition: OperationalDisposition
    incident_id: UUID | None = None
    plan_id: UUID | None = None
    verification_id: UUID | None = None
    approval_id: UUID | None = None
    execution_id: UUID | None = None
    lifecycle: tuple[AuditLifecycleReference, ...] = Field(default_factory=tuple)
    safe_reason_codes: tuple[str, ...] = Field(default_factory=tuple)
    reconciliation_status: AuditReconciliationStatus
    reconciliation_issues: tuple[str, ...] = Field(default_factory=tuple)

    @model_validator(mode="after")
    def validate_sanitized_codes(self) -> "ScenarioOperationalTrace":
        code_pattern = re.compile(r"^[A-Z][A-Z0-9_]{0,99}$")
        if any(
            code_pattern.fullmatch(code) is None
            for code in (*self.safe_reason_codes, *self.reconciliation_issues)
        ):
            raise ValueError("operational trace codes must be sanitized identifiers")
        return self


class EvidenceOperationalSummary(FrozenContract):
    evidence_class: EvidenceClass
    trials: int = Field(ge=0)
    outcomes: OperationalOutcomeCounts
    safe_rejection_reasons: tuple[CodeCount, ...]
    metrics: BenchmarkMetrics
    latency: tuple[StageLatencySummary, ...]
    missing_telemetry: MissingTelemetryCounts


class BenchmarkOperationalSummary(FrozenContract):
    schema_version: Literal[1] = 1
    run_id: UUID
    harness_completed: Literal[True] = True
    benchmark_report_schema_version: int = Field(ge=1)
    total_scenarios: int = Field(ge=0)
    cohorts: tuple[EvidenceOperationalSummary, ...]
    traces: tuple[ScenarioOperationalTrace, ...]
    limitations: tuple[str, ...] = Field(default_factory=tuple)

    @model_validator(mode="after")
    def validate_summary(self) -> "BenchmarkOperationalSummary":
        if sum(cohort.trials for cohort in self.cohorts) != self.total_scenarios:
            raise ValueError("operational cohort counts do not match total scenarios")
        if len(self.traces) != self.total_scenarios:
            raise ValueError("every benchmark scenario requires one operational trace")
        if [cohort.evidence_class for cohort in self.cohorts] != list(EvidenceClass):
            raise ValueError("operational summary requires every ordered evidence class")
        return self


class BenchmarkArtifactFile(FrozenContract):
    name: str = Field(pattern=r"^(?:benchmark|operational-summary)-[0-9a-f-]{36}\.json$")
    schema_version: int = Field(ge=1)
    byte_size: int = Field(gt=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class BenchmarkArtifactManifest(FrozenContract):
    schema_version: Literal[1] = 1
    run_id: UUID
    created_at: datetime
    execution_mode: BenchmarkMode
    evidence_classes: tuple[EvidenceClass, ...]
    files: tuple[BenchmarkArtifactFile, BenchmarkArtifactFile]

    @model_validator(mode="after")
    def validate_manifest(self) -> "BenchmarkArtifactManifest":
        if self.created_at.tzinfo is None:
            raise ValueError("artifact timestamp must be timezone-aware")
        expected_names = (
            f"benchmark-{self.run_id}.json",
            f"operational-summary-{self.run_id}.json",
        )
        if tuple(item.name for item in self.files) != expected_names:
            raise ValueError("artifact manifest requires the ordered report and summary files")
        if tuple(item.schema_version for item in self.files) != (2, 1):
            raise ValueError("artifact manifest contains unsupported report schema versions")
        if tuple(sorted(set(self.evidence_classes), key=lambda item: item.value)) != tuple(
            self.evidence_classes
        ):
            raise ValueError("artifact evidence classes must be unique and ordered")
        return self
