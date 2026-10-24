import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from fluxguard.apps.worker.consumer import (
    EventRecord,
    PermanentEventError,
    apply_event,
    parse_event,
    process_record,
)
from fluxguard.packages.common.models import (
    OrderProjection,
    ProcessedEvent,
    QuarantinedEvent,
)


def event_dict(*, event_id=None, order_id=None, version=1, total="49.98") -> dict:
    order_id = order_id or uuid4()
    return {
        "event_id": str(event_id or uuid4()),
        "event_type": "orders.created.v1",
        "schema_version": version,
        "aggregate_id": str(order_id),
        "occurred_at": datetime.now(UTC).isoformat(),
        "payload": {
            "order_id": str(order_id),
            "customer_id": "cust_123",
            "items": [
                {
                    "product_id": "prod_101",
                    "quantity": 2,
                    "unit_price": "24.99",
                    "line_total": "49.98",
                }
            ],
            "currency": "USD",
            "total": total,
            "created_at": datetime.now(UTC).isoformat(),
        },
    }


def record(data: dict | bytes, offset: int = 1) -> EventRecord:
    value = data if isinstance(data, bytes) else json.dumps(data).encode()
    return EventRecord("orders.created.v1", 0, offset, b"order-key", value)


def test_duplicate_event_has_one_business_effect(session_factory) -> None:
    item = record(event_dict())
    assert process_record(session_factory, item) == "processed"
    assert process_record(session_factory, item) == "duplicate"
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(OrderProjection)) == 1
        assert session.scalar(select(func.count()).select_from(ProcessedEvent)) == 1
        assert session.scalar(select(ProcessedEvent.delivery_count)) == 2


def test_invalid_schema_version_is_quarantined_before_handled(session_factory) -> None:
    result = process_record(session_factory, record(event_dict(version=99), offset=8))
    assert result == "quarantined"
    with session_factory() as session:
        quarantined = session.scalar(select(QuarantinedEvent))
        assert quarantined is not None
        assert "schema_version" in quarantined.error_detail
        assert session.scalar(select(func.count()).select_from(ProcessedEvent)) == 0


def test_malformed_event_is_durably_quarantined_idempotently(session_factory) -> None:
    item = record(b"not-json", offset=9)
    assert process_record(session_factory, item) == "quarantined"
    assert process_record(session_factory, item) == "quarantined"
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(QuarantinedEvent)) == 1


def test_total_invariant_is_checked() -> None:
    with pytest.raises(PermanentEventError, match="total"):
        parse_event(json.dumps(event_dict(total="50.00")).encode())


def test_projection_conflict_rolls_back_processed_marker(session_factory) -> None:
    order_id = uuid4()
    first = parse_event(json.dumps(event_dict(order_id=order_id)).encode())
    conflicting_data = event_dict(order_id=order_id)
    conflicting_data["payload"]["customer_id"] = "other_customer"
    conflicting = parse_event(json.dumps(conflicting_data).encode())
    with session_factory() as session:
        assert apply_event(session, first)
    with session_factory() as session, pytest.raises(PermanentEventError):
        apply_event(session, conflicting)
    with session_factory() as session:
        ids = set(session.scalars(select(ProcessedEvent.event_id)).all())
        assert ids == {first.event_id}
        assert (
            session.scalar(
                select(OrderProjection).where(OrderProjection.order_id == order_id)
            ).customer_id
            == "cust_123"
        )
        assert session.scalar(select(OrderProjection.total)) == Decimal("49.98")
