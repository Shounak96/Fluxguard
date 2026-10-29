import json
from collections import Counter
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict


class SafetyOutcome(StrEnum):
    SUCCESSFUL_RECOVERY = "SUCCESSFUL_RECOVERY"
    VERIFIED_NO_OP = "VERIFIED_NO_OP"
    SAFE_REJECTION = "SAFE_REJECTION"
    OPERATIONAL_FAILURE = "OPERATIONAL_FAILURE"
    INVARIANT_VIOLATION = "INVARIANT_VIOLATION"
    DUPLICATE_BUSINESS_EFFECT = "DUPLICATE_BUSINESS_EFFECT"
    AUDIT_MISMATCH = "AUDIT_MISMATCH"


class FaultCoverage(StrEnum):
    GENUINE_INTEGRATION = "GENUINE_INTEGRATION"
    DETERMINISTIC_SIMULATION = "DETERMINISTIC_SIMULATION"


class SafetyCaseResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario: str
    outcome: SafetyOutcome
    coverage: FaultCoverage
    trials: int = 1
    failures: int = 0
    invariants_preserved: bool
    duplicate_effects: int = 0
    audit_matches: bool
    note: str


class RecoverySafetyReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: int = 1
    cases: tuple[SafetyCaseResult, ...]
    outcome_counts: dict[str, int]
    total_trials: int
    failed_trials: int


def build_safety_report(cases: list[SafetyCaseResult]) -> RecoverySafetyReport:
    ordered = tuple(sorted(cases, key=lambda case: case.scenario))
    counts = Counter(case.outcome.value for case in ordered)
    return RecoverySafetyReport(
        cases=ordered,
        outcome_counts={outcome.value: counts[outcome.value] for outcome in SafetyOutcome},
        total_trials=sum(case.trials for case in ordered),
        failed_trials=sum(case.failures for case in ordered),
    )


def write_safety_report(report: RecoverySafetyReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
