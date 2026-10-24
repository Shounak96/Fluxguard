"""Development-only controlled fault injection against real FluxGuard infrastructure."""

import argparse
import json
import urllib.request
from datetime import UTC, datetime
from uuid import UUID, uuid4

from confluent_kafka import Producer
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from fluxguard.packages.common.config import get_settings
from fluxguard.packages.common.models import OutboxEvent


def _base_event(scenario_id: str) -> dict:
    event_id, order_id = uuid4(), uuid4()
    now = datetime.now(UTC).isoformat()
    return {
        "event_id": str(event_id),
        "event_type": "orders.created.v1",
        "schema_version": 1,
        "aggregate_id": str(order_id),
        "occurred_at": now,
        "payload": {
            "order_id": str(order_id),
            "customer_id": f"fault-{scenario_id}",
            "items": [
                {
                    "product_id": "fault-product",
                    "quantity": 1,
                    "unit_price": "10.00",
                    "line_total": "10.00",
                }
            ],
            "currency": "USD",
            "total": "10.00",
            "created_at": now,
        },
    }


def scenario_payload(name: str, scenario_id: str) -> tuple[bytes, str, str]:
    event = _base_event(scenario_id)
    expected = ""
    if name == "unsupported_schema":
        event["schema_version"] = 99
        event["fault_injection"] = {"scenario": name, "scenario_id": scenario_id}
        expected = "quarantine and UNSUPPORTED_SCHEMA_VERSION incident"
    elif name == "malformed_event":
        expected = "quarantine and MALFORMED_EVENT incident"
        return f'{{"scenario_id":"{scenario_id}",'.encode(), str(uuid4()), expected
    elif name == "invariant_violation":
        event["payload"]["total"] = "11.00"
        event["fault_injection"] = {"scenario": name, "scenario_id": scenario_id}
        expected = "quarantine and BUSINESS_INVARIANT_VIOLATION incident"
    elif name == "poison_message":
        event["fault_injection"] = {"scenario": name, "scenario_id": scenario_id}
        expected = "quarantine and POISON_MESSAGE incident"
    elif name == "transient_processing_failure":
        event["fault_injection"] = {
            "scenario": name,
            "scenario_id": scenario_id,
            "failures": 3,
        }
        expected = "three retries, incident, then one projection"
    elif name == "healthy_event":
        expected = "successful projection after preceding poison messages"
    else:
        raise ValueError(f"unsupported broker scenario: {name}")
    return json.dumps(event, separators=(",", ":")).encode(), event["aggregate_id"], expected


def publish_fault(name: str) -> dict:
    settings = get_settings()
    scenario_id = str(uuid4())
    payload, key, expected = scenario_payload(name, scenario_id)
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "client.id": "fluxguard-fault-injector",
        }
    )
    errors: list[str] = []
    producer.produce(
        settings.kafka_orders_topic,
        key=key,
        value=payload,
        on_delivery=lambda error, _message: errors.append(str(error)) if error else None,
    )
    remaining = producer.flush(10)
    if errors or remaining:
        raise RuntimeError(errors[0] if errors else "fault event was not acknowledged")
    return {"scenario": name, "scenario_id": scenario_id, "expected": expected}


def create_outbox_fault() -> dict:
    settings = get_settings()
    scenario_id = str(uuid4())
    key = f"fault-outbox-{scenario_id}"
    body = {
        "customer_id": "fault-customer",
        "items": [{"product_id": "fault-product", "quantity": 1, "unit_price": "1.00"}],
        "currency": "USD",
    }
    request = urllib.request.Request(
        "http://localhost:8000/api/v1/orders",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        order_id = json.load(response)["data"]["order_id"]
    factory = sessionmaker(bind=create_engine(settings.database_url), expire_on_commit=False)
    with factory() as session, session.begin():
        event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == UUID(order_id)).with_for_update()
        )
        if event is None or event.published_at is not None:
            raise RuntimeError("outbox event was published before fault marker could be applied")
        payload = dict(event.payload)
        payload["_fault_injection"] = {
            "scenario": "outbox_publish_failure",
            "scenario_id": scenario_id,
            "failures": 3,
        }
        event.payload = payload
    return {
        "scenario": "outbox_publish_failure",
        "scenario_id": scenario_id,
        "order_id": order_id,
        "expected": "three controlled publish failures, incident, then successful publication",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "scenario",
        choices=[
            "unsupported_schema",
            "malformed_event",
            "invariant_violation",
            "poison_message",
            "transient_processing_failure",
            "healthy_event",
            "outbox_publish_failure",
            "all",
        ],
    )
    args = parser.parse_args()
    settings = get_settings()
    if settings.environment.lower() == "production" or not settings.fault_injection_enabled:
        raise SystemExit(
            "fault injection is disabled; set ENVIRONMENT=development and "
            "FAULT_INJECTION_ENABLED=true explicitly"
        )
    if args.scenario == "outbox_publish_failure":
        results = [create_outbox_fault()]
    else:
        names = (
            [
                "unsupported_schema",
                "malformed_event",
                "invariant_violation",
                "poison_message",
                "transient_processing_failure",
                "healthy_event",
            ]
            if args.scenario == "all"
            else [args.scenario]
        )
        results = [publish_fault(name) for name in names]
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
