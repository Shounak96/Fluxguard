import argparse
import os
import sys
from pathlib import Path

from fluxguard.packages.common.benchmark_artifacts import (
    BenchmarkArtifactError,
    write_artifact_bundle,
)
from fluxguard.packages.common.benchmark_demo import run_end_to_end_demo
from fluxguard.packages.common.benchmark_metrics import benchmark_report_json
from fluxguard.packages.common.benchmark_observability import build_operational_summary
from fluxguard.packages.common.benchmark_runner import (
    BenchmarkRunnerError,
    IsolationGuardError,
    IsolationSmokeRunner,
    KafkaBenchmarkResources,
    PostgresBenchmarkResources,
    collect_environment,
)
from fluxguard.packages.common.benchmark_scenarios import run_recovery_safety_scenarios
from fluxguard.packages.contracts.benchmarks import (
    BenchmarkMode,
    BenchmarkOperationalSummary,
    BenchmarkReport,
)


def _bounded_trials(value: str) -> int:
    parsed = int(value)
    if not 1 <= parsed <= 100:
        raise argparse.ArgumentTypeError("trial count must be between 1 and 100")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run an isolated FluxGuard benchmark")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["isolation-smoke", "recovery-safety", "end-to-end-demo"],
    )
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--serial-trials", type=_bounded_trials, default=1)
    parser.add_argument("--concurrency-trials", type=_bounded_trials, default=3)
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=os.getenv("BENCHMARK_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="write benchmark-<run-id>.json below this repository-relative directory",
    )
    return parser


def _write_report(
    report: BenchmarkReport,
    output_dir: Path | None,
    *,
    summary: BenchmarkOperationalSummary,
) -> None:
    if output_dir is None:
        sys.stdout.write(benchmark_report_json(report))
        return
    repository_root = Path.cwd().resolve()
    target_directory = (repository_root / output_dir).resolve()
    if not target_directory.is_relative_to(repository_root):
        raise IsolationGuardError("benchmark output directory must remain inside the repository")
    run_directory = write_artifact_bundle(report, summary, target_directory)
    print(run_directory.relative_to(repository_root).as_posix())


def main() -> int:
    arguments = _parser().parse_args()
    if arguments.mode == "end-to-end-demo" and arguments.output_dir is None:
        print(
            "benchmark configuration error: end-to-end-demo requires --output-dir",
            file=sys.stderr,
        )
        return 2
    repository_root = Path.cwd().resolve()
    admin_database_url = os.getenv("BENCHMARK_POSTGRES_ADMIN_URL")
    if not admin_database_url:
        print(
            "benchmark configuration error: BENCHMARK_POSTGRES_ADMIN_URL is required",
            file=sys.stderr,
        )
        return 2
    try:
        postgres = PostgresBenchmarkResources(admin_database_url, repository_root)
        kafka = KafkaBenchmarkResources(arguments.kafka_bootstrap_servers)
        recovery_mode = arguments.mode == "recovery-safety"
        demo_mode = arguments.mode == "end-to-end-demo"
        runner = IsolationSmokeRunner(
            postgres=postgres,
            kafka=kafka,
            environment_collector=lambda revision: collect_environment(repository_root, revision),
            scenario_executor=(
                run_end_to_end_demo
                if demo_mode
                else run_recovery_safety_scenarios
                if recovery_mode
                else None
            ),
            mode=(
                BenchmarkMode.END_TO_END_DEMO
                if demo_mode
                else BenchmarkMode.RECOVERY_SAFETY
                if recovery_mode
                else BenchmarkMode.ISOLATION_SMOKE
            ),
            benchmark_name=(
                "milestone-5-end-to-end-demo"
                if demo_mode
                else "milestone-5-recovery-safety"
                if recovery_mode
                else "milestone-5-isolation-smoke"
            ),
        )
        report = runner.run(
            seed=arguments.seed,
            serial_trials=arguments.serial_trials,
            concurrency_trials=arguments.concurrency_trials,
        )
        summary = build_operational_summary(report)
        _write_report(report, arguments.output_dir, summary=summary)
        return 0
    except KeyboardInterrupt:
        print("benchmark interrupted; owned-resource cleanup was attempted", file=sys.stderr)
        return 130
    except IsolationGuardError as error:
        print(f"benchmark isolation error: {error}", file=sys.stderr)
        return 2
    except BenchmarkRunnerError as error:
        print(f"benchmark failed safely: {error}", file=sys.stderr)
        return 1
    except BenchmarkArtifactError as error:
        print(f"benchmark artifact error: {error}", file=sys.stderr)
        return 1
    except Exception as error:
        print(f"benchmark failed safely: {type(error).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
