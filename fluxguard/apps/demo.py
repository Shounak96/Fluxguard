"""Run the milestone-one demo from inside the API container."""

import json
import time
import urllib.request
from uuid import UUID, uuid4

from sqlalchemy import func, select

from fluxguard.apps.worker.outbox import KafkaEventPublisher, event_bytes
from fluxguard.packages.common.config import get_settings
from fluxguard.packages.common.db import SessionLocal
from fluxguard.packages.common.models import OrderProjection, OutboxEvent, ProcessedEvent


def post_order(body: dict, key: str) -> dict:
    request = urllib.request.Request(
        "http://localhost:8000/api/v1/orders",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def wait_for_projection(order_id, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with SessionLocal() as session:
            if session.get(OrderProjection, order_id) is not None:
                return
        time.sleep(0.25)
    raise TimeoutError("order projection was not created within 20 seconds")


def wait_for_duplicate(event_id, minimum: int, timeout: float = 20) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with SessionLocal() as session:
            delivery_count = session.scalar(
                select(ProcessedEvent.delivery_count).where(ProcessedEvent.event_id == event_id)
            )
            if delivery_count is not None and delivery_count >= minimum:
                return delivery_count
        time.sleep(0.25)
    raise TimeoutError("duplicate event was not consumed within 20 seconds")


def main() -> None:
    settings = get_settings()
    key = f"demo-{uuid4()}"
    body = {
        "customer_id": "cust_123",
        "items": [{"product_id": "prod_101", "quantity": 2, "unit_price": "24.99"}],
        "currency": "USD",
    }
    first = post_order(body, key)
    replay = post_order(body, key)
    order_id_text = first["data"]["order_id"]
    if replay["data"]["order_id"] != order_id_text:
        raise AssertionError("idempotent replay created a different order")
    order_id = UUID(order_id_text)
    wait_for_projection(order_id)

    with SessionLocal() as session:
        outbox = session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == order_id))
        if outbox is None or outbox.published_at is None:
            raise AssertionError("outbox event was not acknowledged as published")
        initial_delivery_count = session.scalar(
            select(ProcessedEvent.delivery_count).where(ProcessedEvent.event_id == outbox.event_id)
        )
        if initial_delivery_count is None:
            raise AssertionError("initial event delivery was not recorded")
        KafkaEventPublisher(settings.kafka_bootstrap_servers).publish(
            settings.kafka_orders_topic, order_id_text, event_bytes(outbox)
        )
    delivery_count = wait_for_duplicate(outbox.event_id, initial_delivery_count + 1)

    with SessionLocal() as session:
        projection_count = session.scalar(
            select(func.count())
            .select_from(OrderProjection)
            .where(OrderProjection.order_id == order_id)
        )
        processed_count = session.scalar(
            select(func.count())
            .select_from(ProcessedEvent)
            .where(ProcessedEvent.event_id == outbox.event_id)
        )
    result = {
        "order_id": order_id_text,
        "first_submission": first["meta"]["status"],
        "second_submission": replay["meta"]["status"],
        "outbox_published_at": outbox.published_at.isoformat(),
        "duplicate_event_republished": True,
        "projection_rows_for_order": projection_count,
        "processed_rows_for_event": processed_count,
        "observed_event_deliveries": delivery_count,
    }
    print(json.dumps(result, indent=2))
    if projection_count != 1 or processed_count != 1:
        raise AssertionError("duplicate delivery produced duplicate database effects")


if __name__ == "__main__":
    main()
