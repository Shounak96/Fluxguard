import json
import os
import platform
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

import psycopg
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg import sql
from sqlalchemy import create_engine, make_url

from fluxguard.packages.common.benchmark_metrics import build_benchmark_report
from fluxguard.packages.common.config import get_settings
from fluxguard.packages.contracts.benchmarks import (
    BenchmarkConfiguration,
    BenchmarkEnvironment,
    BenchmarkMode,
    BenchmarkReport,
    BenchmarkResourceNames,
    BenchmarkRunMetadata,
    ScenarioResult,
)

OWNERSHIP_MARKER_VERSION = 1
OWNERSHIP_TABLE = "fluxguard_benchmark_ownership"
LOCAL_DATABASE_HOSTS = frozenset({"localhost", "127.0.0.1", "postgres"})
LOCAL_KAFKA_HOSTS = frozenset({"localhost", "127.0.0.1", "redpanda"})


class BenchmarkRunnerError(RuntimeError):
    pass


class IsolationGuardError(BenchmarkRunnerError):
    pass


class ResourceCollisionError(BenchmarkRunnerError):
    pass


class ResourceOwnershipError(BenchmarkRunnerError):
    pass


class ResourceCleanupError(BenchmarkRunnerError):
    pass


@dataclass(frozen=True)
class OwnedBenchmarkResources:
    run_id: UUID
    ownership_token: UUID
    names: BenchmarkResourceNames

    def validate(self) -> None:
        token = self.run_id.hex
        if self.names != benchmark_resource_names(self.run_id):
            raise ResourceOwnershipError("resource names do not match the owning benchmark run")
        if token not in self.names.database_name:
            raise ResourceOwnershipError("database name does not contain its owning run ID")


@dataclass(frozen=True)
class OwnershipMarker:
    run_id: UUID
    database_name: str
    ownership_token: UUID
    marker_version: int


def benchmark_resource_names(run_id: UUID) -> BenchmarkResourceNames:
    token = run_id.hex
    return BenchmarkResourceNames(
        database_name=f"fluxguard_benchmark_{token}",
        kafka_topic=f"fluxguard-benchmark-{token}-orders",
        kafka_consumer_group=f"fluxguard-benchmark-{token}-consumer",
    )


def new_owned_resources(
    *,
    run_id_factory: Callable[[], UUID] = uuid4,
    ownership_token_factory: Callable[[], UUID] = uuid4,
) -> OwnedBenchmarkResources:
    run_id = run_id_factory()
    resources = OwnedBenchmarkResources(
        run_id=run_id,
        ownership_token=ownership_token_factory(),
        names=benchmark_resource_names(run_id),
    )
    resources.validate()
    return resources


def validate_ownership_marker(
    marker: OwnershipMarker | None,
    ownership: OwnedBenchmarkResources,
) -> None:
    ownership.validate()
    if marker is None:
        raise ResourceOwnershipError("disposable database ownership marker is missing")
    expected = OwnershipMarker(
        run_id=ownership.run_id,
        database_name=ownership.names.database_name,
        ownership_token=ownership.ownership_token,
        marker_version=OWNERSHIP_MARKER_VERSION,
    )
    if marker != expected:
        raise ResourceOwnershipError("disposable database ownership marker does not match this run")


def validate_admin_database_url(value: str) -> None:
    try:
        url = make_url(value)
    except Exception as error:
        raise IsolationGuardError("benchmark PostgreSQL maintenance URL is invalid") from error
    if not url.drivername.startswith("postgresql"):
        raise IsolationGuardError("benchmark resources require PostgreSQL")
    if url.database != "postgres":
        raise IsolationGuardError(
            "benchmark maintenance connection must target the postgres database"
        )
    if (url.host or "").lower() not in LOCAL_DATABASE_HOSTS:
        raise IsolationGuardError("benchmark PostgreSQL must use an approved local Docker host")
    if not url.username:
        raise IsolationGuardError("benchmark PostgreSQL maintenance URL requires a user")


def validate_kafka_bootstrap_servers(value: str) -> None:
    servers = [item.strip() for item in value.split(",") if item.strip()]
    if not servers:
        raise IsolationGuardError("benchmark Kafka bootstrap servers are required")
    for server in servers:
        host, separator, port = server.rpartition(":")
        if not separator or host.lower() not in LOCAL_KAFKA_HOSTS or not port.isdigit():
            raise IsolationGuardError("benchmark Kafka must use approved local Docker endpoints")


def _psycopg_dsn(value: str) -> str:
    return make_url(value).set(drivername="postgresql").render_as_string(hide_password=False)


class PostgresBenchmarkResources:
    def __init__(self, admin_database_url: str, repository_root: Path) -> None:
        validate_admin_database_url(admin_database_url)
        self.admin_database_url = admin_database_url
        self.repository_root = repository_root.resolve()
        self.created = False
        self.marker_created = False
        self._ownership: OwnedBenchmarkResources | None = None

    def _target_url(self, ownership: OwnedBenchmarkResources) -> str:
        ownership.validate()
        return (
            make_url(self.admin_database_url)
            .set(database=ownership.names.database_name)
            .render_as_string(hide_password=False)
        )

    def target_database_url(self, ownership: OwnedBenchmarkResources) -> str:
        self.verify_ownership(ownership)
        return self._target_url(ownership)

    def database_exists(self, database_name: str) -> bool:
        with (
            psycopg.connect(_psycopg_dsn(self.admin_database_url), autocommit=True) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
            return cursor.fetchone() is not None

    def create(self, ownership: OwnedBenchmarkResources) -> None:
        ownership.validate()
        if self.created or self._ownership is not None:
            raise ResourceOwnershipError("PostgreSQL lifecycle already owns a benchmark database")
        if self.database_exists(ownership.names.database_name):
            raise ResourceCollisionError("generated benchmark database already exists")
        with (
            psycopg.connect(_psycopg_dsn(self.admin_database_url), autocommit=True) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(ownership.names.database_name))
            )
        self.created = True
        self._ownership = ownership
        try:
            with (
                psycopg.connect(_psycopg_dsn(self._target_url(ownership))) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute(
                    sql.SQL(
                        "CREATE TABLE {} ("
                        "marker_version integer PRIMARY KEY, "
                        "run_id uuid NOT NULL UNIQUE, "
                        "database_name text NOT NULL UNIQUE, "
                        "ownership_token uuid NOT NULL UNIQUE, "
                        "created_at timestamptz NOT NULL"
                        ")"
                    ).format(sql.Identifier(OWNERSHIP_TABLE))
                )
                cursor.execute(
                    sql.SQL(
                        "INSERT INTO {} "
                        "(marker_version, run_id, database_name, ownership_token, created_at) "
                        "VALUES (%s, %s, %s, %s, %s)"
                    ).format(sql.Identifier(OWNERSHIP_TABLE)),
                    (
                        OWNERSHIP_MARKER_VERSION,
                        ownership.run_id,
                        ownership.names.database_name,
                        ownership.ownership_token,
                        datetime.now(UTC),
                    ),
                )
            self.marker_created = True
        except Exception as error:
            raise ResourceOwnershipError(
                "benchmark database was created but its ownership marker could not be persisted"
            ) from error

    def _load_marker(self, ownership: OwnedBenchmarkResources) -> OwnershipMarker | None:
        with (
            psycopg.connect(_psycopg_dsn(self._target_url(ownership))) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                sql.SQL(
                    "SELECT run_id, database_name, ownership_token, marker_version FROM {}"
                ).format(sql.Identifier(OWNERSHIP_TABLE))
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return OwnershipMarker(
            run_id=row[0],
            database_name=row[1],
            ownership_token=row[2],
            marker_version=row[3],
        )

    def verify_ownership(self, ownership: OwnedBenchmarkResources) -> None:
        if not self.created or self._ownership != ownership or not self.marker_created:
            raise ResourceOwnershipError("PostgreSQL resource is not owned by this benchmark run")
        validate_ownership_marker(self._load_marker(ownership), ownership)

    def apply_migrations(self, ownership: OwnedBenchmarkResources) -> str | None:
        self.verify_ownership(ownership)
        target_url = self._target_url(ownership)
        previous_url = os.environ.get("DATABASE_URL")
        try:
            os.environ["DATABASE_URL"] = target_url
            get_settings.cache_clear()
            alembic = Config(str(self.repository_root / "alembic.ini"))
            alembic.set_main_option("script_location", str(self.repository_root / "migrations"))
            alembic.set_main_option("sqlalchemy.url", target_url)
            command.upgrade(alembic, "head")
        finally:
            if previous_url is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = previous_url
            get_settings.cache_clear()
        self.verify_ownership(ownership)
        engine = create_engine(target_url, pool_pre_ping=True)
        try:
            with engine.connect() as connection:
                return MigrationContext.configure(connection).get_current_revision()
        finally:
            engine.dispose()

    def cleanup(self, ownership: OwnedBenchmarkResources) -> None:
        if not self.created:
            return
        self.verify_ownership(ownership)
        with (
            psycopg.connect(_psycopg_dsn(self.admin_database_url), autocommit=True) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (ownership.names.database_name,),
            )
            cursor.execute(
                sql.SQL("DROP DATABASE {}").format(sql.Identifier(ownership.names.database_name))
            )
        self.created = False
        self.marker_created = False
        self._ownership = None


class KafkaBenchmarkResources:
    def __init__(self, bootstrap_servers: str) -> None:
        validate_kafka_bootstrap_servers(bootstrap_servers)
        self.bootstrap_servers = bootstrap_servers
        self.admin = AdminClient(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "fluxguard-benchmark-isolation",
            }
        )
        self.topic_created = False
        self._group_ids: set[str] = set()
        self._ownership: OwnedBenchmarkResources | None = None

    def topic_exists(self, topic: str) -> bool:
        return topic in self.admin.list_topics(timeout=10).topics

    def create(self, ownership: OwnedBenchmarkResources) -> None:
        ownership.validate()
        if self._ownership is not None:
            raise ResourceOwnershipError("Kafka lifecycle already owns benchmark resources")
        if self.topic_exists(ownership.names.kafka_topic):
            raise ResourceCollisionError("generated benchmark topic already exists")
        future = self.admin.create_topics(
            [NewTopic(ownership.names.kafka_topic, num_partitions=1, replication_factor=1)],
            operation_timeout=10,
            request_timeout=15,
        )[ownership.names.kafka_topic]
        future.result(timeout=15)
        self.topic_created = True
        self._ownership = ownership

    def mark_group_created(self, ownership: OwnedBenchmarkResources, group_id: str) -> None:
        if self._ownership != ownership:
            raise ResourceOwnershipError("Kafka consumer group is not owned by this run")
        prefix = f"{ownership.names.kafka_consumer_group}-"
        if group_id != ownership.names.kafka_consumer_group and not group_id.startswith(prefix):
            raise ResourceOwnershipError("Kafka consumer group is outside the owned run namespace")
        self._group_ids.add(group_id)

    def group_exists(self, group_id: str) -> bool:
        result = self.admin.list_consumer_groups(request_timeout=10).result(timeout=10)
        return any(group.group_id == group_id for group in result.valid)

    def cleanup(self, ownership: OwnedBenchmarkResources) -> None:
        if self._ownership is None:
            return
        ownership.validate()
        if self._ownership != ownership:
            raise ResourceOwnershipError("Kafka resources are not owned by this benchmark run")
        if self._group_ids:
            futures = self.admin.delete_consumer_groups(sorted(self._group_ids), request_timeout=15)
            for group_id in sorted(self._group_ids):
                futures[group_id].result(timeout=15)
            self._group_ids.clear()
        if self.topic_created:
            future = self.admin.delete_topics(
                [ownership.names.kafka_topic],
                operation_timeout=10,
                request_timeout=15,
            )[ownership.names.kafka_topic]
            future.result(timeout=15)
            self.topic_created = False
        self._ownership = None


class BenchmarkWorker(Protocol):
    name: str
    topic: str
    group_id: str

    def start(self) -> None: ...

    def stop(self) -> None: ...


class BenchmarkWorkerController:
    def __init__(self, ownership: OwnedBenchmarkResources) -> None:
        ownership.validate()
        self.ownership = ownership
        self._started: list[BenchmarkWorker] = []

    def start(self, worker: BenchmarkWorker) -> None:
        if worker.topic != self.ownership.names.kafka_topic:
            raise IsolationGuardError("benchmark worker cannot consume a shared topic")
        if worker.group_id != self.ownership.names.kafka_consumer_group:
            raise IsolationGuardError("benchmark worker cannot use a shared consumer group")
        try:
            worker.start()
        except BaseException:
            worker.stop()
            raise
        self._started.append(worker)

    def stop_all(self) -> None:
        errors = []
        while self._started:
            worker = self._started.pop()
            try:
                worker.stop()
            except Exception as error:
                errors.append(type(error).__name__)
        if errors:
            raise ResourceCleanupError(f"{len(errors)} benchmark worker(s) failed to stop cleanly")


class PostgresLifecycle(Protocol):
    def create(self, ownership: OwnedBenchmarkResources) -> None: ...

    def apply_migrations(self, ownership: OwnedBenchmarkResources) -> str | None: ...

    def target_database_url(self, ownership: OwnedBenchmarkResources) -> str: ...

    def cleanup(self, ownership: OwnedBenchmarkResources) -> None: ...


class KafkaLifecycle(Protocol):
    bootstrap_servers: str

    def create(self, ownership: OwnedBenchmarkResources) -> None: ...

    def mark_group_created(self, ownership: OwnedBenchmarkResources, group_id: str) -> None: ...

    def cleanup(self, ownership: OwnedBenchmarkResources) -> None: ...


EnvironmentCollector = Callable[[str | None], BenchmarkEnvironment]
SmokeProbe = Callable[[OwnedBenchmarkResources, BenchmarkWorkerController], None]


@dataclass(frozen=True)
class BenchmarkScenarioContext:
    ownership: OwnedBenchmarkResources
    database_url: str
    kafka_bootstrap_servers: str
    workers: BenchmarkWorkerController
    seed: int
    serial_trials: int
    concurrency_trials: int
    register_kafka_group: Callable[[str], None]


ScenarioExecutor = Callable[[BenchmarkScenarioContext], list[ScenarioResult]]


class IsolationSmokeRunner:
    def __init__(
        self,
        *,
        postgres: PostgresLifecycle,
        kafka: KafkaLifecycle,
        environment_collector: EnvironmentCollector,
        smoke_probe: SmokeProbe | None = None,
        scenario_executor: ScenarioExecutor | None = None,
        mode: BenchmarkMode = BenchmarkMode.ISOLATION_SMOKE,
        benchmark_name: str = "milestone-5-isolation-smoke",
        run_id_factory: Callable[[], UUID] = uuid4,
        ownership_token_factory: Callable[[], UUID] = uuid4,
        utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.postgres = postgres
        self.kafka = kafka
        self.environment_collector = environment_collector
        self.smoke_probe = smoke_probe or (lambda _ownership, _workers: None)
        self.scenario_executor = scenario_executor
        self.mode = mode
        self.benchmark_name = benchmark_name
        self.run_id_factory = run_id_factory
        self.ownership_token_factory = ownership_token_factory
        self.utc_now = utc_now
        self.monotonic = monotonic

    def run(
        self,
        *,
        seed: int,
        serial_trials: int,
        concurrency_trials: int,
    ) -> BenchmarkReport:
        ownership = new_owned_resources(
            run_id_factory=self.run_id_factory,
            ownership_token_factory=self.ownership_token_factory,
        )
        workers = BenchmarkWorkerController(ownership)
        started_at = self.utc_now()
        started_clock = self.monotonic()
        migration_revision = None
        environment = None
        results: list[ScenarioResult] = []
        primary_error: BaseException | None = None
        cleanup_errors: list[Exception] = []
        try:
            self.postgres.create(ownership)
            migration_revision = self.postgres.apply_migrations(ownership)
            self.kafka.create(ownership)
            self.smoke_probe(ownership, workers)
            if self.scenario_executor is not None:
                results = self.scenario_executor(
                    BenchmarkScenarioContext(
                        ownership=ownership,
                        database_url=self.postgres.target_database_url(ownership),
                        kafka_bootstrap_servers=self.kafka.bootstrap_servers,
                        workers=workers,
                        seed=seed,
                        serial_trials=serial_trials,
                        concurrency_trials=concurrency_trials,
                        register_kafka_group=lambda group_id: self.kafka.mark_group_created(
                            ownership, group_id
                        ),
                    )
                )
            environment = self.environment_collector(migration_revision)
        except BaseException as error:
            primary_error = error
        finally:
            for cleanup in (
                workers.stop_all,
                lambda: self.kafka.cleanup(ownership),
                lambda: self.postgres.cleanup(ownership),
            ):
                try:
                    cleanup()
                except Exception as error:
                    cleanup_errors.append(error)

        if cleanup_errors:
            cleanup_error = ResourceCleanupError(
                f"{len(cleanup_errors)} owned resource cleanup operation(s) failed"
            )
            if primary_error is not None:
                raise cleanup_error from primary_error
            raise cleanup_error from cleanup_errors[0]
        if primary_error is not None:
            raise primary_error.with_traceback(primary_error.__traceback__)
        if environment is None:
            raise BenchmarkRunnerError("environment collection did not produce metadata")

        completed_at = self.utc_now()
        duration_ms = max(0.0, (self.monotonic() - started_clock) * 1000)
        run = BenchmarkRunMetadata(
            run_id=ownership.run_id,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=duration_ms,
        )
        configuration = BenchmarkConfiguration(
            benchmark_name=self.benchmark_name,
            mode=self.mode,
            seed=seed,
            serial_trials=serial_trials,
            concurrency_trials=concurrency_trials,
            provider_calls_enabled=False,
            resources=ownership.names,
        )
        return build_benchmark_report(
            run=run,
            configuration=configuration,
            environment=environment,
            results=results,
            limitations=[
                (
                    "isolation smoke mode; no recovery or investigation scenarios executed"
                    if self.mode == BenchmarkMode.ISOLATION_SMOKE
                    else "local single-node PostgreSQL and Redpanda benchmark"
                ),
            ],
        )


def _command_output(
    arguments: list[str],
    repository_root: Path,
    *,
    empty_is_none: bool = True,
) -> str | None:
    try:
        result = subprocess.run(
            arguments,
            cwd=repository_root,
            capture_output=True,
            check=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    value = result.stdout.strip()
    return value or (None if empty_is_none else "")


def collect_environment(
    repository_root: Path,
    migration_revision: str | None,
) -> BenchmarkEnvironment:
    repository_root = repository_root.resolve()
    git_sha = _command_output(["git", "rev-parse", "HEAD"], repository_root)
    dirty_output = _command_output(
        ["git", "status", "--porcelain"], repository_root, empty_is_none=False
    )
    docker_version = _command_output(
        ["docker", "version", "--format", "{{.Server.Version}}"], repository_root
    )
    compose_version = _command_output(["docker", "compose", "version", "--short"], repository_root)
    postgres_image = None
    redpanda_image = None
    compose_config = _command_output(
        ["docker", "compose", "config", "--format", "json"], repository_root
    )
    if compose_config is not None:
        try:
            services = json.loads(compose_config).get("services", {})
            postgres_image = services.get("postgres", {}).get("image")
            redpanda_image = services.get("redpanda", {}).get("image")
        except (AttributeError, json.JSONDecodeError):
            pass
    return BenchmarkEnvironment(
        git_sha=git_sha,
        dirty_worktree=None if dirty_output is None else bool(dirty_output),
        python_version=platform.python_version(),
        docker_version=docker_version,
        compose_version=compose_version,
        postgres_image=postgres_image,
        redpanda_image=redpanda_image,
        migration_revision=migration_revision,
    )
