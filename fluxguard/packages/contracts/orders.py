from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class OrderItemInput(BaseModel):
    product_id: str = Field(min_length=1, max_length=100)
    quantity: int = Field(gt=0, le=1_000_000)
    unit_price: Decimal = Field(ge=0, max_digits=12, decimal_places=2)

    @field_validator("unit_price")
    @classmethod
    def require_cent_precision(cls, value: Decimal) -> Decimal:
        if value.as_tuple().exponent < -2:
            raise ValueError("unit_price must have at most two decimal places")
        return value.quantize(Decimal("0.01"))


class OrderCreateRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    customer_id: str = Field(min_length=1, max_length=100)
    items: list[OrderItemInput] = Field(min_length=1, max_length=1_000)
    currency: str = Field(min_length=3, max_length=3)

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        normalized = value.upper()
        if not normalized.isalpha() or not normalized.isascii():
            raise ValueError("currency must be a three-letter ASCII code")
        return normalized


class OrderItemView(BaseModel):
    product_id: str = Field(min_length=1, max_length=100)
    quantity: int = Field(gt=0, le=1_000_000)
    unit_price: Decimal = Field(ge=0, max_digits=12, decimal_places=2)
    line_total: Decimal = Field(ge=0, max_digits=18, decimal_places=2)


class OrderView(BaseModel):
    order_id: UUID
    customer_id: str
    items: list[OrderItemView]
    currency: str
    total: Decimal
    schema_version: int
    created_at: datetime


class OrderResponse(BaseModel):
    data: OrderView
    meta: dict[str, str] = Field(default_factory=lambda: {"status": "accepted"})


class OrderCreatedPayload(BaseModel):
    order_id: UUID
    customer_id: str = Field(min_length=1, max_length=100)
    items: list[OrderItemView] = Field(min_length=1)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    total: Decimal = Field(ge=0, max_digits=22, decimal_places=2)
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def require_aware_created_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value
