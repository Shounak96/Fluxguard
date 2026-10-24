from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator

from fluxguard.packages.contracts.orders import OrderCreatedPayload

ORDER_CREATED_EVENT_TYPE = "orders.created.v1"
ORDER_SCHEMA_VERSION = 1


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: Literal["orders.created.v1"]
    schema_version: Literal[1]
    aggregate_id: UUID
    occurred_at: datetime
    payload: dict[str, Any]


class OrderCreatedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: Literal["orders.created.v1"] = ORDER_CREATED_EVENT_TYPE
    schema_version: Literal[1] = ORDER_SCHEMA_VERSION
    aggregate_id: UUID
    occurred_at: datetime
    payload: OrderCreatedPayload

    @field_validator("occurred_at")
    @classmethod
    def require_aware_occurred_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must be timezone-aware")
        return value
