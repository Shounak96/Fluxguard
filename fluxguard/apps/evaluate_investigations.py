import argparse
import sys
from pathlib import Path

from fluxguard.packages.common.db import SessionLocal
from fluxguard.packages.common.investigation_evaluation import (
    evaluate_historical,
    terminal_summary,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate persisted FluxGuard investigations without invoking an LLM."
    )
    parser.add_argument(
        "--json-output",
        default="-",
        help="JSON destination path, or '-' for stdout (default).",
    )
    args = parser.parse_args()
    with SessionLocal() as session:
        report = evaluate_historical(session)
    payload = report.model_dump_json(indent=2)
    summary = terminal_summary(report)
    if args.json_output == "-":
        print(payload)
        print(summary, file=sys.stderr)
        return
    destination = Path(args.json_output)
    destination.write_text(payload + "\n", encoding="utf-8")
    print(summary)
    print(f"JSON: {destination}")


if __name__ == "__main__":
    main()
