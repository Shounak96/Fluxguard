import hashlib
import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

from pydantic import ValidationError

from fluxguard.packages.common.benchmark_observability import build_operational_summary
from fluxguard.packages.contracts.benchmarks import (
    AuditReconciliationStatus,
    BenchmarkArtifactFile,
    BenchmarkArtifactManifest,
    BenchmarkOperationalSummary,
    BenchmarkReport,
    OperationalDisposition,
)

MANIFEST_NAME = "manifest.json"
_FORBIDDEN_KEYS = frozenset(
    {
        "api_key",
        "authorization",
        "connection_string",
        "credentials",
        "database_url",
        "event_payload",
        "password",
        "raw_payload",
        "raw_response",
        "secret",
    }
)
_SENSITIVE_VALUE = re.compile(
    r"(?:postgres(?:ql)?|mysql|mongodb|redis|https?)://[^\s\"']+"
    r"|bearer\s+\S+"
    r"|-----begin(?: [a-z]+)? private key-----"
    r"|\bsk-[a-z0-9_-]{16,}",
    re.IGNORECASE,
)


class BenchmarkArtifactError(RuntimeError):
    pass


@dataclass(frozen=True)
class VerifiedArtifactBundle:
    run_id: str
    execution_mode: str
    scenario_count: int
    evidence_classes: tuple[str, ...]

    def json(self) -> str:
        return (
            json.dumps(
                {
                    "evidence_classes": self.evidence_classes,
                    "execution_mode": self.execution_mode,
                    "run_id": self.run_id,
                    "scenario_count": self.scenario_count,
                    "schema_version": 1,
                    "valid": True,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )


def _stable_json(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode()


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _scan_sensitive(value: object, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower()
            if normalized in _FORBIDDEN_KEYS:
                raise BenchmarkArtifactError(f"sensitive field is not permitted at {path}.{key}")
            _scan_sensitive(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _scan_sensitive(item, f"{path}[{index}]")
    elif isinstance(value, str) and _SENSITIVE_VALUE.search(value):
        raise BenchmarkArtifactError(f"sensitive value is not permitted at {path}")


def _artifact_entry(name: str, data: bytes, schema_version: int) -> BenchmarkArtifactFile:
    return BenchmarkArtifactFile(
        name=name,
        schema_version=schema_version,
        byte_size=len(data),
        sha256=_digest(data),
    )


def write_artifact_bundle(
    report: BenchmarkReport,
    summary: BenchmarkOperationalSummary,
    output_directory: Path,
) -> Path:
    if summary != build_operational_summary(report):
        raise BenchmarkArtifactError("operational summary does not match benchmark report")
    root = output_directory.resolve()
    root.mkdir(parents=True, exist_ok=True)
    run_directory = root / str(report.run.run_id)
    partial_directory = root / f".{report.run.run_id}.partial"
    if run_directory.exists() or partial_directory.exists():
        raise BenchmarkArtifactError("artifact run directory already exists")

    report_name = f"benchmark-{report.run.run_id}.json"
    summary_name = f"operational-summary-{report.run.run_id}.json"
    report_data = _stable_json(report.model_dump(mode="json"))
    summary_data = _stable_json(summary.model_dump(mode="json"))
    evidence_classes = tuple(
        sorted(
            {result.identity.evidence_class for result in report.results},
            key=lambda item: item.value,
        )
    )
    manifest = BenchmarkArtifactManifest(
        run_id=report.run.run_id,
        created_at=report.run.completed_at,
        execution_mode=report.configuration.mode,
        evidence_classes=evidence_classes,
        files=(
            _artifact_entry(report_name, report_data, report.schema_version),
            _artifact_entry(summary_name, summary_data, summary.schema_version),
        ),
    )
    manifest_data = _stable_json(manifest.model_dump(mode="json"))

    partial_directory.mkdir()
    try:
        (partial_directory / report_name).write_bytes(report_data)
        (partial_directory / summary_name).write_bytes(summary_data)
        (partial_directory / MANIFEST_NAME).write_bytes(manifest_data)
        os.replace(partial_directory, run_directory)
    except BaseException:
        if partial_directory.exists():
            shutil.rmtree(partial_directory)
        raise
    return run_directory


def _read_json(path: Path) -> object:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BenchmarkArtifactError(f"artifact is missing or invalid: {path.name}") from error
    _scan_sensitive(payload)
    return payload


def verify_artifact_bundle(run_directory: Path) -> VerifiedArtifactBundle:
    directory = run_directory.resolve()
    if not directory.is_dir():
        raise BenchmarkArtifactError("artifact run directory does not exist")
    try:
        manifest = BenchmarkArtifactManifest.model_validate(_read_json(directory / MANIFEST_NAME))
    except ValidationError as error:
        raise BenchmarkArtifactError(
            "artifact manifest schema is unsupported or invalid"
        ) from error

    expected_files = {MANIFEST_NAME, *(item.name for item in manifest.files)}
    actual_files = {path.name for path in directory.iterdir() if path.is_file()}
    if actual_files != expected_files:
        raise BenchmarkArtifactError("artifact file set does not match the manifest")

    payloads: dict[str, object] = {}
    for item in manifest.files:
        path = directory / item.name
        try:
            data = path.read_bytes()
        except OSError as error:
            raise BenchmarkArtifactError(f"manifest file is missing: {item.name}") from error
        if len(data) != item.byte_size or _digest(data) != item.sha256:
            raise BenchmarkArtifactError(f"artifact checksum or size mismatch: {item.name}")
        payloads[item.name] = _read_json(path)

    try:
        report = BenchmarkReport.model_validate(payloads[manifest.files[0].name])
        summary = BenchmarkOperationalSummary.model_validate(payloads[manifest.files[1].name])
    except ValidationError as error:
        raise BenchmarkArtifactError("artifact report schema is unsupported or invalid") from error

    if report.run.run_id != manifest.run_id or summary.run_id != manifest.run_id:
        raise BenchmarkArtifactError("artifact run IDs do not match")
    if report.configuration.mode != manifest.execution_mode:
        raise BenchmarkArtifactError("artifact execution mode does not match")
    report_evidence = tuple(
        sorted(
            {result.identity.evidence_class for result in report.results},
            key=lambda item: item.value,
        )
    )
    if report_evidence != manifest.evidence_classes:
        raise BenchmarkArtifactError("artifact evidence classes do not match")
    expected_summary = build_operational_summary(report)
    if summary != expected_summary:
        raise BenchmarkArtifactError("operational summary does not match benchmark outcomes")
    if any(
        trace.reconciliation_status == AuditReconciliationStatus.INCONSISTENT
        for trace in summary.traces
    ):
        raise BenchmarkArtifactError("artifact contains an inconsistent audit trace")
    unsafe_dispositions = {
        OperationalDisposition.SAFETY_ASSERTION_FAILURE,
        OperationalDisposition.UNEXPECTED_OPERATIONAL_FAILURE,
    }
    if any(trace.disposition in unsafe_dispositions for trace in summary.traces):
        raise BenchmarkArtifactError("artifact contains a safety or unexpected operational failure")

    return VerifiedArtifactBundle(
        run_id=str(manifest.run_id),
        execution_mode=manifest.execution_mode.value,
        scenario_count=len(report.results),
        evidence_classes=tuple(item.value for item in manifest.evidence_classes),
    )
