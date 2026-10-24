from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "fluxguard"
    environment: str = "development"
    log_level: str = "INFO"
    database_url: str = (
        "postgresql+psycopg://fluxguard:fluxguard_local_only@localhost:15432/fluxguard"
    )
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_orders_topic: str = "orders.created.v1"
    kafka_consumer_group: str = "fluxguard-order-projection-v1"
    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=50, gt=0, le=500)
    kafka_poll_timeout_seconds: float = Field(default=1.0, gt=0)
    incident_api_enabled: bool = False
    fault_injection_enabled: bool = False
    consumer_retry_incident_threshold: int = Field(default=3, gt=0, le=100)
    outbox_failure_incident_threshold: int = Field(default=3, gt=0, le=100)


@lru_cache
def get_settings() -> Settings:
    return Settings()
