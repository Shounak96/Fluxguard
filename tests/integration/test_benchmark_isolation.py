import os
import time
from pathlib import Path

import pytest

from fluxguard.packages.common.benchmark_runner import (
    IsolationSmokeRunner,
    KafkaBenchmarkResources,
    PostgresBenchmarkResources,
    collect_environment,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_BENCHMARK_INTEGRATION") != "1",
        reason="set RUN_BENCHMARK_INTEGRATION=1 with local PostgreSQL and Redpanda",
    ),
]


def test_real_isolation_smoke_creates_migrates_and_removes_owned_resources() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    postgres = PostgresBenchmarkResources(
        os.environ["BENCHMARK_POSTGRES_ADMIN_URL"], repository_root
    )
    kafka = KafkaBenchmarkResources(os.environ["BENCHMARK_KAFKA_BOOTSTRAP_SERVERS"])
    runner = IsolationSmokeRunner(
        postgres=postgres,
        kafka=kafka,
        environment_collector=lambda revision: collect_environment(repository_root, revision),
    )

    report = runner.run(seed=1, serial_trials=1, concurrency_trials=3)

    assert report.results == ()
    assert report.environment.migration_revision == "0006_recovery_execution"
    assert not postgres.database_exists(report.configuration.resources.database_name)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and kafka.topic_exists(
        report.configuration.resources.kafka_topic
    ):
        time.sleep(0.25)
    assert not kafka.topic_exists(report.configuration.resources.kafka_topic)
    assert all(
        cohort.metrics.operational_completion.rate_percent is None for cohort in report.cohorts
    )
