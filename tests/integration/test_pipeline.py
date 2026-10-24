import os
import time
from uuid import uuid4

import pytest
from confluent_kafka import Consumer
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from fluxguard.apps.api.service import create_order
from fluxguard.apps.worker.consumer import EventRecord, process_record
from fluxguard.apps.worker.outbox import KafkaEventPublisher, publish_pending
from fluxguard.packages.common.models import OrderProjection, OutboxEvent
from fluxguard.packages.contracts.orders import OrderCreateRequest

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="set RUN_INTEGRATION=1 with migrated PostgreSQL and running Redpanda",
)
def test_real_postgres_redpanda_pipeline() -> None:
    database_url = os.environ["DATABASE_URL"]
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic = os.getenv("KAFKA_ORDERS_TOPIC", "orders.created.v1")
    factory = sessionmaker(bind=create_engine(database_url), expire_on_commit=False)
    key = f"integration-{uuid4()}"
    request = OrderCreateRequest.model_validate(
        {
            "customer_id": "integration_customer",
            "items": [{"product_id": "p1", "quantity": 2, "unit_price": "3.50"}],
            "currency": "USD",
        }
    )
    with factory() as session:
        order, _ = create_order(session, request, key)
    assert publish_pending(factory, KafkaEventPublisher(brokers), topic) >= 1

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"fluxguard-integration-{uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    deadline = time.monotonic() + 20
    handled = False
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None or message.error() or message.key() != str(order.order_id).encode():
                continue
            record = EventRecord(
                message.topic(),
                message.partition(),
                message.offset(),
                message.key(),
                message.value(),
            )
            assert process_record(factory, record) == "processed"
            assert process_record(factory, record) == "duplicate"
            handled = True
            break
    finally:
        consumer.close()
    assert handled, "published integration event was not observed"
    with factory() as session:
        assert session.get(OrderProjection, order.order_id) is not None
        assert (
            session.scalar(
                select(func.count())
                .select_from(OrderProjection)
                .where(OrderProjection.order_id == order.order_id)
            )
            == 1
        )
        assert (
            session.scalar(
                select(OutboxEvent.published_at).where(OutboxEvent.aggregate_id == order.order_id)
            )
            is not None
        )
