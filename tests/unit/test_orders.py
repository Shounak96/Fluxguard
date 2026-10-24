from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, func, select

from fluxguard.apps.api.main import create_app
from fluxguard.apps.api.service import calculate_order_total, create_order
from fluxguard.packages.common.models import Order, OutboxEvent
from fluxguard.packages.contracts.orders import OrderCreateRequest


def payload(price: str = "24.99") -> dict:
    return {
        "customer_id": "cust_123",
        "items": [{"product_id": "prod_101", "quantity": 2, "unit_price": price}],
        "currency": "usd",
    }


def client_for(session_factory) -> TestClient:
    def sessions():
        with session_factory() as session:
            yield session

    return TestClient(create_app(sessions))


def test_server_calculates_money_with_decimal() -> None:
    request = OrderCreateRequest.model_validate(payload())
    assert calculate_order_total(request) == Decimal("49.98")
    assert request.currency == "USD"


@pytest.mark.parametrize(
    "change",
    [
        {"items": []},
        {"currency": "US"},
        {"items": [{"product_id": "p", "quantity": 0, "unit_price": "1.00"}]},
        {"items": [{"product_id": "p", "quantity": 1, "unit_price": "1.001"}]},
    ],
)
def test_request_validation_rejects_invalid_values(session_factory, change) -> None:
    body = payload()
    body.update(change)
    response = client_for(session_factory).post(
        "/api/v1/orders", json=body, headers={"Idempotency-Key": "validation"}
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


def test_order_and_outbox_are_created_atomically_and_replayed(session_factory) -> None:
    client = client_for(session_factory)
    first = client.post("/api/v1/orders", json=payload(), headers={"Idempotency-Key": "key-123"})
    second = client.post("/api/v1/orders", json=payload(), headers={"Idempotency-Key": "key-123"})
    assert first.status_code == second.status_code == 201
    assert first.json()["data"]["order_id"] == second.json()["data"]["order_id"]
    assert first.json()["data"]["total"] == "49.98"
    assert first.json()["meta"]["status"] == "accepted"
    assert second.json()["meta"]["status"] == "replayed"
    assert first.headers["X-Correlation-ID"]
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Order)) == 1
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def test_idempotency_key_conflicts_on_changed_payload(session_factory) -> None:
    client = client_for(session_factory)
    assert (
        client.post(
            "/api/v1/orders", json=payload(), headers={"Idempotency-Key": "same-key"}
        ).status_code
        == 201
    )
    response = client.post(
        "/api/v1/orders",
        json=payload("25.00"),
        headers={"Idempotency-Key": "same-key"},
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "idempotency_conflict"


def test_idempotency_header_is_required(session_factory) -> None:
    response = client_for(session_factory).post("/api/v1/orders", json=payload())
    assert response.status_code == 422


def test_order_rolls_back_if_outbox_insert_fails(session_factory) -> None:
    def fail_outbox_insert(*_args) -> None:
        raise RuntimeError("injected outbox failure")

    event.listen(OutboxEvent, "before_insert", fail_outbox_insert)
    try:
        with session_factory() as session, pytest.raises(RuntimeError, match="injected"):
            create_order(session, OrderCreateRequest.model_validate(payload()), "rollback-key")
    finally:
        event.remove(OutboxEvent, "before_insert", fail_outbox_insert)

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Order)) == 0
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
