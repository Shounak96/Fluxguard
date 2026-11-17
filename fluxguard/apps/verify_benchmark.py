import argparse
import sys
from pathlib import Path

from fluxguard.packages.common.benchmark_artifacts import (
    BenchmarkArtifactError,
    verify_artifact_bundle,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify a packaged FluxGuard benchmark run")
    parser.add_argument("run_directory", type=Path)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser


def _resolve_directory(path: Path) -> Path:
    root = Path.cwd().resolve()
    resolved = (root / path).resolve()
    if not resolved.is_relative_to(root):
        raise BenchmarkArtifactError("artifact directory must remain inside the repository")
    return resolved


def main() -> int:
    arguments = _parser().parse_args()
    try:
        result = verify_artifact_bundle(_resolve_directory(arguments.run_directory))
    except BenchmarkArtifactError as error:
        print(f"benchmark artifact verification failed: {error}", file=sys.stderr)
        return 1
    if arguments.format == "json":
        sys.stdout.write(result.json())
    else:
        evidence = ",".join(result.evidence_classes) or "none"
        print(
            f"verified run={result.run_id} mode={result.execution_mode} "
            f"scenarios={result.scenario_count} evidence={evidence}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
