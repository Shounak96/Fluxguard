import argparse
import sys
from pathlib import Path

from fluxguard.packages.common.benchmark_observability import (
    BenchmarkObservabilityError,
    build_operational_summary,
    load_benchmark_report,
    operational_summary_json,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize an existing FluxGuard benchmark report without executing a benchmark"
    )
    parser.add_argument("report", type=Path)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser


def _resolve_report(path: Path) -> Path:
    root = Path.cwd().resolve()
    resolved = (root / path).resolve()
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise BenchmarkObservabilityError(
            "benchmark report must be an existing file inside the repository"
        )
    return resolved


def _text_summary(summary) -> str:
    lines = [
        f"run={summary.run_id} completed={str(summary.harness_completed).lower()} "
        f"scenarios={summary.total_scenarios}"
    ]
    for cohort in summary.cohorts:
        if cohort.trials == 0:
            continue
        outcomes = cohort.outcomes
        lines.append(
            f"{cohort.evidence_class.value}: trials={cohort.trials} "
            f"recovered={outcomes.recovery_execution_success} "
            f"no_op={outcomes.verified_no_op} rejected={outcomes.safe_rejection} "
            f"expected_conflicts={outcomes.expected_operational_conflict} "
            f"expected_failures={outcomes.expected_operational_failure} "
            f"unexpected_failures={outcomes.unexpected_operational_failure} "
            f"safety_failures={outcomes.safety_assertion_failure}"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    arguments = _parser().parse_args()
    try:
        report = load_benchmark_report(_resolve_report(arguments.report))
        summary = build_operational_summary(report)
    except BenchmarkObservabilityError as error:
        print(f"benchmark observability error: {error}", file=sys.stderr)
        return 2
    if arguments.format == "json":
        sys.stdout.write(operational_summary_json(summary))
    else:
        sys.stdout.write(_text_summary(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
