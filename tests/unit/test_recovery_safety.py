import json

from fluxguard.packages.common.recovery_safety import (
    FaultCoverage,
    SafetyCaseResult,
    SafetyOutcome,
    build_safety_report,
    write_safety_report,
)


def test_safety_report_is_deterministic_and_keeps_failure_categories_separate(tmp_path) -> None:
    cases = [
        SafetyCaseResult(
            scenario="valid-replay",
            outcome=SafetyOutcome.SUCCESSFUL_RECOVERY,
            coverage=FaultCoverage.GENUINE_INTEGRATION,
            invariants_preserved=True,
            audit_matches=True,
            note="real PostgreSQL transaction",
        ),
        SafetyCaseResult(
            scenario="crash-before-ack",
            outcome=SafetyOutcome.VERIFIED_NO_OP,
            coverage=FaultCoverage.DETERMINISTIC_SIMULATION,
            trials=3,
            invariants_preserved=True,
            audit_matches=True,
            note="commit/retry boundary simulated; no process was killed",
        ),
    ]
    report = build_safety_report(list(reversed(cases)))
    output = tmp_path / "report.json"
    write_safety_report(report, output)
    decoded = json.loads(output.read_text())
    assert [case["scenario"] for case in decoded["cases"]] == [
        "crash-before-ack",
        "valid-replay",
    ]
    assert decoded["outcome_counts"]["SUCCESSFUL_RECOVERY"] == 1
    assert decoded["outcome_counts"]["VERIFIED_NO_OP"] == 1
    assert decoded["outcome_counts"]["INVARIANT_VIOLATION"] == 0
    assert decoded["total_trials"] == 4
