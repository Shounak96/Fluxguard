import hashlib
import json
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from fluxguard.packages.common.models import Order, OrderItem, OutboxEvent
from fluxguard.packages.common.time import utc_now
from fluxguard.packages.contracts.events import (
    ORDER_CREATED_EVENT_TYPE,
    ORDER_SCHEMA_VERSION,
)
from fluxguard.packages.contracts.orders import (
    OrderCreatedPayload,
    OrderCreateRequest,
    OrderItemView,
    OrderView,
)


class IdempotencyConflictError(Exception):
    pass


def calculate_line_total(quantity: int, unit_price: Decimal) -> Decimal:
    return (Decimal(quantity) * unit_price).quantize(Decimal("0.01"))


def calculate_order_total(request: OrderCreateRequest) -> Decimal:
    return sum(
        (calculate_line_total(item.quantity, item.unit_price) for item in request.items),
        start=Decimal("0.00"),
    ).quantize(Decimal("0.01"))


def request_fingerprint(request: OrderCreateRequest) -> str:
    canonical = json.dumps(
        request.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def order_to_view(order: Order) -> OrderView:
    return OrderView(
        order_id=order.id,
        customer_id=order.customer_id,
        items=[
            OrderItemView(
                product_id=item.product_id,
                quantity=item.quantity,
                unit_price=item.unit_price,
                line_total=item.line_total,
            )
            for item in sorted(order.items, key=lambda value: value.position)
        ],
        currency=order.currency,
        total=order.total,
        schema_version=order.schema_version,
        created_at=order.created_at,
    )


def create_order(
    session: Session, request: OrderCreateRequest, idempotency_key: str
) -> tuple[OrderView, bool]:
    fingerprint = request_fingerprint(request)
    try:
        with session.begin():
            existing = session.scalar(select(Order).where(Order.idempotency_key == idempotency_key))
            if existing is not None:
                if existing.request_hash != fingerprint:
                    raise IdempotencyConflictError(
                        "the idempotency key was already used with a different request"
                    )
                return order_to_view(existing), True

            order_id, event_id, now = uuid4(), uuid4(), utc_now()
            items = [
                OrderItem(
                    position=index,
                    product_id=item.product_id,
                    quantity=item.quantity,
                    unit_price=item.unit_price,
                    line_total=calculate_line_total(item.quantity, item.unit_price),
                )
                for index, item in enumerate(request.items)
            ]
            total = calculate_order_total(request)
            order = Order(
                id=order_id,
                customer_id=request.customer_id,
                currency=request.currency,
                total=total,
                schema_version=ORDER_SCHEMA_VERSION,
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
                created_at=now,
                items=items,
            )
            payload = OrderCreatedPayload(
                order_id=order_id,
                customer_id=request.customer_id,
                items=[
                    OrderItemView(
                        product_id=item.product_id,
                        quantity=item.quantity,
                        unit_price=item.unit_price,
                        line_total=calculate_line_total(item.quantity, item.unit_price),
                    )
                    for item in request.items
                ],
                currency=request.currency,
                total=total,
                created_at=now,
            )
            outbox = OutboxEvent(
                event_id=event_id,
                event_type=ORDER_CREATED_EVENT_TYPE,
                schema_version=ORDER_SCHEMA_VERSION,
                aggregate_id=order_id,
                occurred_at=now,
                payload=payload.model_dump(mode="json"),
            )
            session.add_all([order, outbox])
            session.flush()
    except IntegrityError:
        session.rollback()
        existing = session.scalar(select(Order).where(Order.idempotency_key == idempotency_key))
        if existing is None:
            raise
        if existing.request_hash != fingerprint:
            raise IdempotencyConflictError(
                "the idempotency key was already used with a different request"
            ) from None
        return order_to_view(existing), True
    return order_to_view(order), False


def get_order(session: Session, order_id: UUID) -> Order | None:
    return session.get(Order, order_id)
