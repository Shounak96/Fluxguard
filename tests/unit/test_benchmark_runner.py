import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from fluxguard.apps import benchmark as benchmark_cli
from fluxguard.apps import verify_benchmark as verify_benchmark_cli
from fluxguard.packages.common.benchmark_artifacts import (
    BenchmarkArtifactError,
    verify_artifact_bundle,
    write_artifact_bundle,
)
from fluxguard.packages.common.benchmark_observability import build_operational_summary
from fluxguard.packages.common.benchmark_runner import (
    BenchmarkWorkerController,
    IsolationGuardError,
    IsolationSmokeRunner,
    OwnedBenchmarkResources,
    OwnershipMarker,
    ResourceCollisionError,
    ResourceOwnershipError,
    benchmark_resource_names,
    new_owned_resources,
    validate_admin_database_url,
    validate_kafka_bootstrap_servers,
    validate_ownership_marker,
)
from fluxguard.packages.contracts.benchmarks import BenchmarkEnvironment, EvidenceClass

RUN_ID = UUID("12345678-1234-4234-8234-123456789abc")
OWNER_TOKEN = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")


def ownership() -> OwnedBenchmarkResources:
    return OwnedBenchmarkResources(
        run_id=RUN_ID,
        ownership_token=OWNER_TOKEN,
        names=benchmark_resource_names(RUN_ID),
    )


class FakePostgres:
    def __init__(self, events: list[str], *, fail_create: bool = False) -> None:
        self.events = events
        self.fail_create = fail_create
        self.created = False
        self.cleaned = False

    def create(self, owned) -> None:
        self.events.append("postgres.create")
        if self.fail_create:
            raise ResourceCollisionError("database exists")
        self.created = True

    def apply_migrations(self, owned) -> str:
        assert self.created
        self.events.append("postgres.migrate")
        return "0006_recovery_execution"

    def target_database_url(self, owned) -> str:
        return f"postgresql+psycopg://benchmark@postgres/{owned.names.database_name}"

    def cleanup(self, owned) -> None:
        self.events.append("postgres.cleanup")
        if self.created:
            self.cleaned = True
            self.created = False


class FakeKafka:
    def __init__(self, events: list[str], *, fail_create: bool = False) -> None:
        self.events = events
        self.fail_create = fail_create
        self.created = False
        self.cleaned = False
        self.bootstrap_servers = "redpanda:9092"

    def create(self, owned) -> None:
        self.events.append("kafka.create")
        if self.fail_create:
            raise RuntimeError("broker setup failed")
        self.created = True

    def mark_group_created(self, owned, group_id) -> None:
        self.events.append(f"kafka.group:{group_id}")

    def cleanup(self, owned) -> None:
        self.events.append("kafka.cleanup")
        if self.created:
            self.cleaned = True
            self.created = False


class FakeWorker:
    def __init__(
        self,
        *,
        name: str,
        topic: str,
        group_id: str,
        events: list[str],
    ) -> None:
        self.name = name
        self.topic = topic
        self.group_id = group_id
        self.events = events

    def start(self) -> None:
        self.events.append(f"{self.name}.start")

    def stop(self) -> None:
        self.events.append(f"{self.name}.stop")


def environment(revision: str | None) -> BenchmarkEnvironment:
    return BenchmarkEnvironment(
        python_version="3.12.7",
        migration_revision=revision,
    )


def runner(
    postgres,
    kafka,
    *,
    probe=None,
    run_id_factory=lambda: RUN_ID,
) -> IsolationSmokeRunner:
    moments = iter(
        [
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=1),
        ]
    )
    clocks = iter([10.0, 11.0])
    return IsolationSmokeRunner(
        postgres=postgres,
        kafka=kafka,
        environment_collector=environment,
        smoke_probe=probe,
        run_id_factory=run_id_factory,
        ownership_token_factory=lambda: OWNER_TOKEN,
        utc_now=lambda: next(moments),
        monotonic=lambda: next(clocks),
    )


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg://user:password@localhost:15432/fluxguard",
        "postgresql+psycopg://user:password@localhost:15432/production",
        "postgresql+psycopg://user:password@db.example.com:5432/postgres",
        "sqlite:///benchmark.db",
    ],
)
def test_isolation_guard_rejects_application_or_nonlocal_databases(url) -> None:
    with pytest.raises(IsolationGuardError):
        validate_admin_database_url(url)


def test_local_maintenance_and_broker_endpoints_are_explicitly_allowed() -> None:
    validate_admin_database_url(
        "postgresql+psycopg://fluxguard:local-only@localhost:15432/postgres"
    )
    validate_kafka_bootstrap_servers("localhost:19092")
    with pytest.raises(IsolationGuardError):
        validate_kafka_bootstrap_servers("redpanda.example.com:9092")


def test_ownership_marker_must_exist_and_match_exactly() -> None:
    owned = ownership()
    with pytest.raises(ResourceOwnershipError):
        validate_ownership_marker(None, owned)
    with pytest.raises(ResourceOwnershipError):
        validate_ownership_marker(
            OwnershipMarker(
                run_id=owned.run_id,
                database_name=owned.names.database_name,
                ownership_token=uuid4(),
                marker_version=1,
            ),
            owned,
        )
    validate_ownership_marker(
        OwnershipMarker(
            run_id=owned.run_id,
            database_name=owned.names.database_name,
            ownership_token=owned.ownership_token,
            marker_version=1,
        ),
        owned,
    )


def test_existing_database_collision_stops_before_broker_setup() -> None:
    events: list[str] = []
    postgres = FakePostgres(events, fail_create=True)
    kafka = FakeKafka(events)

    with pytest.raises(ResourceCollisionError):
        runner(postgres, kafka).run(seed=1, serial_trials=1, concurrency_trials=3)

    assert events == ["postgres.create", "kafka.cleanup", "postgres.cleanup"]
    assert kafka.created is False


def test_run_ids_and_all_resource_names_are_unique() -> None:
    first = new_owned_resources()
    second = new_owned_resources()

    assert first.run_id != second.run_id
    assert first.names.database_name != second.names.database_name
    assert first.names.kafka_topic != second.names.kafka_topic
    assert first.names.kafka_consumer_group != second.names.kafka_consumer_group
    assert "orders.created.v1" not in {first.names.kafka_topic, second.names.kafka_topic}
    assert "fluxguard-order-projection-v1" not in {
        first.names.kafka_consumer_group,
        second.names.kafka_consumer_group,
    }


def test_scenario_context_registers_only_through_owned_kafka_lifecycle() -> None:
    events: list[str] = []
    postgres = FakePostgres(events)
    kafka = FakeKafka(events)

    def scenario(context):
        context.register_kafka_group(f"{context.ownership.names.kafka_consumer_group}-demo")
        return []

    IsolationSmokeRunner(
        postgres=postgres,
        kafka=kafka,
        environment_collector=environment,
        scenario_executor=scenario,
        run_id_factory=lambda: RUN_ID,
        ownership_token_factory=lambda: OWNER_TOKEN,
    ).run(seed=1, serial_trials=1, concurrency_trials=1)

    assert f"kafka.group:{ownership().names.kafka_consumer_group}-demo" in events


def test_worker_controller_allows_only_owned_names_and_stops_in_reverse_order() -> None:
    owned = ownership()
    events: list[str] = []
    controller = BenchmarkWorkerController(owned)
    first = FakeWorker(
        name="first",
        topic=owned.names.kafka_topic,
        group_id=owned.names.kafka_consumer_group,
        events=events,
    )
    second = FakeWorker(
        name="second",
        topic=owned.names.kafka_topic,
        group_id=owned.names.kafka_consumer_group,
        events=events,
    )
    controller.start(first)
    controller.start(second)
    with pytest.raises(IsolationGuardError):
        controller.start(
            FakeWorker(
                name="development",
                topic="orders.created.v1",
                group_id="fluxguard-order-projection-v1",
                events=events,
            )
        )
    controller.stop_all()

    assert events == ["first.start", "second.start", "second.stop", "first.stop"]


def test_partial_setup_failure_cleans_database_after_worker_shutdown_boundary() -> None:
    events: list[str] = []
    postgres = FakePostgres(events)
    kafka = FakeKafka(events, fail_create=True)

    with pytest.raises(RuntimeError, match="broker setup failed"):
        runner(postgres, kafka).run(seed=1, serial_trials=1, concurrency_trials=3)

    assert events == [
        "postgres.create",
        "postgres.migrate",
        "kafka.create",
        "kafka.cleanup",
        "postgres.cleanup",
    ]
    assert postgres.cleaned is True


@pytest.mark.parametrize("failure", [RuntimeError("probe failed"), KeyboardInterrupt()])
def test_exception_and_interruption_both_clean_owned_resources(failure) -> None:
    events: list[str] = []
    postgres = FakePostgres(events)
    kafka = FakeKafka(events)

    def fail_probe(_owned, _workers) -> None:
        raise failure

    with pytest.raises(type(failure)):
        runner(postgres, kafka, probe=fail_probe).run(seed=1, serial_trials=1, concurrency_trials=3)

    assert kafka.cleaned is True
    assert postgres.cleaned is True
    assert events[-2:] == ["kafka.cleanup", "postgres.cleanup"]


def test_runner_stops_owned_worker_before_resource_cleanup() -> None:
    events: list[str] = []
    postgres = FakePostgres(events)
    kafka = FakeKafka(events)

    def start_worker(owned, workers) -> None:
        workers.start(
            FakeWorker(
                name="benchmark-worker",
                topic=owned.names.kafka_topic,
                group_id=owned.names.kafka_consumer_group,
                events=events,
            )
        )

    runner(postgres, kafka, probe=start_worker).run(seed=1, serial_trials=1, concurrency_trials=3)

    assert events[-3:] == [
        "benchmark-worker.stop",
        "kafka.cleanup",
        "postgres.cleanup",
    ]


def test_smoke_report_is_empty_versioned_and_has_null_quality_rates() -> None:
    events: list[str] = []
    report = runner(FakePostgres(events), FakeKafka(events)).run(
        seed=9,
        serial_trials=1,
        concurrency_trials=3,
    )

    assert report.schema_version == 2
    assert report.run.run_id == RUN_ID
    assert report.run.duration_ms == 1000
    assert report.results == ()
    assert report.configuration.provider_calls_enabled is False
    assert report.environment.migration_revision == "0006_recovery_execution"
    assert [item.evidence_class for item in report.cohorts] == list(EvidenceClass)
    for item in report.cohorts:
        assert item.trials == 0
        assert item.metrics.safe_recovery_success.rate_percent is None
        assert item.metrics.operational_completion.rate_percent is None


def test_cli_requires_explicit_admin_connection_without_prompt(monkeypatch, capsys) -> None:
    monkeypatch.delenv("BENCHMARK_POSTGRES_ADMIN_URL", raising=False)
    monkeypatch.setattr(sys, "argv", ["fluxguard-benchmark", "--mode", "isolation-smoke"])

    assert benchmark_cli.main() == 2
    assert "BENCHMARK_POSTGRES_ADMIN_URL is required" in capsys.readouterr().err


def test_demo_mode_requires_explicit_repository_output_directory(monkeypatch, capsys) -> None:
    monkeypatch.setattr(sys, "argv", ["fluxguard-benchmark", "--mode", "end-to-end-demo"])

    assert benchmark_cli.main() == 2
    assert "end-to-end-demo requires --output-dir" in capsys.readouterr().err


def test_cli_writes_valid_empty_report_to_stdout(monkeypatch, capsys) -> None:
    events: list[str] = []
    report = runner(FakePostgres(events), FakeKafka(events)).run(
        seed=1, serial_trials=1, concurrency_trials=3
    )

    class FixedRunner:
        def __init__(self, **_kwargs) -> None:
            pass

        def run(self, **_kwargs):
            return report

    monkeypatch.setenv(
        "BENCHMARK_POSTGRES_ADMIN_URL",
        "postgresql+psycopg://local:local@localhost:15432/postgres",
    )
    monkeypatch.setattr(benchmark_cli, "PostgresBenchmarkResources", lambda *_args: object())
    monkeypatch.setattr(benchmark_cli, "KafkaBenchmarkResources", lambda *_args: object())
    monkeypatch.setattr(benchmark_cli, "IsolationSmokeRunner", FixedRunner)
    monkeypatch.setattr(sys, "argv", ["fluxguard-benchmark", "--mode", "isolation-smoke"])

    assert benchmark_cli.main() == 0
    output = json.loads(capsys.readouterr().out)
    assert output["schema_version"] == 2
    assert output["results"] == []


def test_output_directory_receives_report_and_operational_summary(
    tmp_path, monkeypatch, capsys
) -> None:
    monkeypatch.chdir(tmp_path)
    report = runner(FakePostgres([]), FakeKafka([])).run(
        seed=1, serial_trials=1, concurrency_trials=3
    )

    benchmark_cli._write_report(
        report,
        Path("build/demo"),
        summary=build_operational_summary(report),
    )

    run_directory = tmp_path / "build" / "demo" / str(RUN_ID)
    report_path = run_directory / f"benchmark-{RUN_ID}.json"
    summary_path = run_directory / f"operational-summary-{RUN_ID}.json"
    assert json.loads(report_path.read_text(encoding="utf-8"))["schema_version"] == 2
    assert json.loads(summary_path.read_text(encoding="utf-8"))["schema_version"] == 1
    assert (
        json.loads((run_directory / "manifest.json").read_text(encoding="utf-8"))["schema_version"]
        == 1
    )
    assert str(RUN_ID) in capsys.readouterr().out


def _bundle(tmp_path: Path):
    report = runner(FakePostgres([]), FakeKafka([])).run(
        seed=1, serial_trials=1, concurrency_trials=3
    )
    directory = write_artifact_bundle(
        report,
        build_operational_summary(report),
        tmp_path,
    )
    return report, directory


def _rewrite_manifest_entry(directory: Path, artifact_name: str) -> None:
    artifact = directory / artifact_name
    data = artifact.read_bytes()
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = next(item for item in manifest["files"] if item["name"] == artifact_name)
    entry["byte_size"] = len(data)
    entry["sha256"] = hashlib.sha256(data).hexdigest()
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def test_artifact_manifest_and_checksums_verify(tmp_path) -> None:
    report, directory = _bundle(tmp_path)

    verified = verify_artifact_bundle(directory)

    assert verified.run_id == str(report.run.run_id)
    assert verified.scenario_count == 0
    assert verified.evidence_classes == ()


@pytest.mark.parametrize("change", ["missing", "modified"])
def test_artifact_verifier_detects_missing_or_modified_report(tmp_path, change) -> None:
    report, directory = _bundle(tmp_path)
    report_path = directory / f"benchmark-{report.run.run_id}.json"
    if change == "missing":
        report_path.unlink()
    else:
        report_path.write_text("{}\n", encoding="utf-8")

    with pytest.raises(BenchmarkArtifactError):
        verify_artifact_bundle(directory)


def test_artifact_verifier_rejects_run_id_mismatch(tmp_path) -> None:
    report, directory = _bundle(tmp_path)
    summary_name = f"operational-summary-{report.run.run_id}.json"
    summary_path = directory / summary_name
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["run_id"] = str(uuid4())
    summary_path.write_text(
        json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    _rewrite_manifest_entry(directory, summary_name)

    with pytest.raises(BenchmarkArtifactError, match="run IDs"):
        verify_artifact_bundle(directory)


def test_artifact_verifier_rejects_unsupported_report_schema(tmp_path) -> None:
    report, directory = _bundle(tmp_path)
    report_name = f"benchmark-{report.run.run_id}.json"
    report_path = directory / report_name
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["schema_version"] = 99
    report_path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    _rewrite_manifest_entry(directory, report_name)

    with pytest.raises(BenchmarkArtifactError, match="schema"):
        verify_artifact_bundle(directory)


def test_artifact_verifier_rejects_unsupported_manifest_schema(tmp_path) -> None:
    _, directory = _bundle(tmp_path)
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = 99
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(BenchmarkArtifactError, match="manifest schema"):
        verify_artifact_bundle(directory)


def test_artifact_verifier_rejects_outcome_summary_mismatch(tmp_path) -> None:
    report, directory = _bundle(tmp_path)
    summary_name = f"operational-summary-{report.run.run_id}.json"
    summary_path = directory / summary_name
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["cohorts"][0]["outcomes"]["safe_rejection"] = 1
    summary_path.write_text(
        json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    _rewrite_manifest_entry(directory, summary_name)

    with pytest.raises(BenchmarkArtifactError, match="summary"):
        verify_artifact_bundle(directory)


def test_artifact_verifier_rejects_sensitive_fields(tmp_path) -> None:
    report, directory = _bundle(tmp_path)
    report_name = f"benchmark-{report.run.run_id}.json"
    report_path = directory / report_name
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["api_key"] = "not-a-real-key"
    report_path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    _rewrite_manifest_entry(directory, report_name)

    with pytest.raises(BenchmarkArtifactError, match="sensitive field"):
        verify_artifact_bundle(directory)


def test_artifact_writer_removes_partial_output_on_failure(tmp_path, monkeypatch) -> None:
    report = runner(FakePostgres([]), FakeKafka([])).run(
        seed=1, serial_trials=1, concurrency_trials=3
    )
    original = Path.write_bytes
    writes = 0

    def fail_second_write(path, data):
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("controlled write failure")
        return original(path, data)

    monkeypatch.setattr(Path, "write_bytes", fail_second_write)

    with pytest.raises(OSError, match="controlled write failure"):
        write_artifact_bundle(report, build_operational_summary(report), tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_verification_cli_returns_failure_status_for_invalid_bundle(
    tmp_path, monkeypatch, capsys
) -> None:
    monkeypatch.chdir(tmp_path)
    invalid = tmp_path / "invalid"
    invalid.mkdir()
    monkeypatch.setattr(sys, "argv", ["fluxguard-verify-benchmark", "invalid"])

    assert verify_benchmark_cli.main() == 1
    assert "verification failed" in capsys.readouterr().err
