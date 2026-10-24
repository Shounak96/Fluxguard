import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from fluxguard.apps.worker.consumer import EventRecord, process_record
from fluxguard.packages.common.config import Settings
from fluxguard.packages.common.incidents import Detection, incident_fingerprint, record_incident
from fluxguard.packages.common.models import (
    Incident,
    IncidentEvidence,
    IncidentSeverity,
    OrderProjection,
    QuarantinedEvent,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1",
        reason="set RUN_INTEGRATION=1 with migrated PostgreSQL and running Redpanda",
    ),
]


def test_real_fault_incident_evidence_and_healthy_following_event() -> None:
    database_url = os.environ["DATABASE_URL"]
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic = os.getenv("KAFKA_ORDERS_TOPIC", "orders.created.v1")
    factory = sessionmaker(bind=create_engine(database_url), expire_on_commit=False)
    key = f"incident-integration-{uuid4()}"
    producer = Producer({"bootstrap.servers": brokers, "acks": "all"})
    producer.produce(topic, key=key, value=b'{"broken":')
    order_id, event_id = uuid4(), uuid4()
    now = datetime.now(UTC).isoformat()
    valid = {
        "event_id": str(event_id),
        "event_type": "orders.created.v1",
        "schema_version": 1,
        "aggregate_id": str(order_id),
        "occurred_at": now,
        "payload": {
            "order_id": str(order_id),
            "customer_id": "integration-private",
            "items": [
                {
                    "product_id": "p1",
                    "quantity": 1,
                    "unit_price": "2.00",
                    "line_total": "2.00",
                }
            ],
            "currency": "USD",
            "total": "2.00",
            "created_at": now,
        },
    }
    producer.produce(topic, key=key, value=json.dumps(valid).encode())
    assert producer.flush(10) == 0

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"fluxguard-incident-integration-{uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    outcomes: list[str] = []
    records: list[EventRecord] = []
    deadline = time.monotonic() + 20
    try:
        while time.monotonic() < deadline and len(outcomes) < 2:
            message = consumer.poll(1)
            if message is None or message.error():
                continue
            if message.key() != key.encode():
                consumer.commit(message=message, asynchronous=False)
                continue
            item = EventRecord(
                message.topic(),
                message.partition(),
                message.offset(),
                message.key(),
                message.value(),
            )
            records.append(item)
            outcomes.append(process_record(factory, item, Settings(fault_injection_enabled=True)))
            consumer.commit(message=message, asynchronous=False)
        assert outcomes == ["quarantined", "processed"]
        assert (
            process_record(factory, records[1], Settings(fault_injection_enabled=True))
            == "duplicate"
        )
        committed = consumer.committed([TopicPartition(topic, 0)], timeout=5)[0].offset
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=5)
        assert committed == high
    finally:
        consumer.close()

    with factory() as session:
        incident = session.scalar(
            select(Incident)
            .join(IncidentEvidence)
            .where(
                Incident.error_code == "MALFORMED_JSON",
                IncidentEvidence.kafka_topic == topic,
                IncidentEvidence.kafka_partition == records[0].partition,
                IncidentEvidence.kafka_offset == records[0].offset,
            )
        )
        assert incident is not None
        assert (
            session.scalar(
                select(func.count())
                .select_from(IncidentEvidence)
                .where(
                    IncidentEvidence.incident_id == incident.incident_id,
                    IncidentEvidence.kafka_topic == topic,
                    IncidentEvidence.kafka_partition == records[0].partition,
                    IncidentEvidence.kafka_offset == records[0].offset,
                )
            )
            == 1
        )
        assert (
            session.scalar(
                select(QuarantinedEvent.id).where(
                    QuarantinedEvent.topic == topic,
                    QuarantinedEvent.partition == records[0].partition,
                    QuarantinedEvent.offset == records[0].offset,
                )
            )
            is not None
        )
        assert session.get(OrderProjection, order_id) is not None
        assert (
            session.scalar(
                select(func.count())
                .select_from(OrderProjection)
                .where(OrderProjection.order_id == order_id)
            )
            == 1
        )


def test_concurrent_incident_correlation_on_postgres() -> None:
    factory = sessionmaker(bind=create_engine(os.environ["DATABASE_URL"]), expire_on_commit=False)
    scenario_id = str(uuid4())

    def write_occurrence(offset: int) -> None:
        detection = Detection(
            incident_type="POISON_MESSAGE",
            severity=IncidentSeverity.HIGH,
            error_code="CONCURRENT_CORRELATION_TEST",
            summary="concurrent correlation test",
            source_component="integration_test",
            fingerprint_scope={"scenario_id": scenario_id},
            evidence_type="TEST_EVIDENCE",
            evidence_payload={"offset": offset},
            topic="orders.created.v1",
            partition=0,
            offset=offset,
        )
        with factory() as session, session.begin():
            record_incident(session, detection)

    with ThreadPoolExecutor(max_workers=5) as executor:
        list(executor.map(write_occurrence, range(10_000, 10_005)))

    probe = Detection(
        incident_type="POISON_MESSAGE",
        severity=IncidentSeverity.HIGH,
        error_code="CONCURRENT_CORRELATION_TEST",
        summary="concurrent correlation test",
        source_component="integration_test",
        fingerprint_scope={"scenario_id": scenario_id},
        evidence_type="TEST_EVIDENCE",
    )
    with factory() as session:
        incident = session.scalar(
            select(Incident).where(Incident.fingerprint == incident_fingerprint(probe))
        )
        assert incident.occurrence_count == 5
        assert (
            session.scalar(
                select(func.count())
                .select_from(IncidentEvidence)
                .where(IncidentEvidence.incident_id == incident.incident_id)
            )
            == 5
        )
