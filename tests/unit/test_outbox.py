from datetime import timedelta

from sqlalchemy import select

from fluxguard.apps.api.service import create_order
from fluxguard.apps.worker.outbox import publish_pending
from fluxguard.packages.common.models import OutboxEvent
from fluxguard.packages.common.time import utc_now
from fluxguard.packages.contracts.orders import OrderCreateRequest


class CapturingPublisher:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.messages: list[tuple[str, str, bytes]] = []

    def publish(self, topic: str, key: str, value: bytes) -> None:
        if self.failures:
            self.failures -= 1
            raise ConnectionError("broker unavailable")
        self.messages.append((topic, key, value))


def request() -> OrderCreateRequest:
    return OrderCreateRequest.model_validate(
        {
            "customer_id": "c1",
            "items": [{"product_id": "p1", "quantity": 1, "unit_price": "1.25"}],
            "currency": "USD",
        }
    )


def test_failed_publication_remains_pending_then_restart_succeeds(session_factory) -> None:
    with session_factory() as session:
        create_order(session, request(), "outbox-retry")

    publisher = CapturingPublisher(failures=1)
    assert publish_pending(session_factory, publisher, "orders.created.v1") == 0
    with session_factory() as session, session.begin():
        event = session.scalar(select(OutboxEvent))
        assert event.published_at is None
        assert event.attempts == 1
        assert "broker unavailable" in event.last_error
        event.next_attempt_at = utc_now() - timedelta(seconds=1)

    assert publish_pending(session_factory, publisher, "orders.created.v1") == 1
    with session_factory() as session:
        event = session.scalar(select(OutboxEvent))
        assert event.published_at is not None
        assert event.attempts == 2
        assert len(publisher.messages) == 1
