from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

GEMINI_FREE_MODEL_IDS = frozenset({"gemini-3.5-flash-lite"})


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
    investigation_api_enabled: bool = False
    recovery_api_enabled: bool = False
    llm_provider: Literal["openrouter", "gemini"] = "openrouter"
    openrouter_api_key: SecretStr | None = None
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    investigation_model_id: str = "nvidia/nemotron-3.5-super-vl:free"
    gemini_api_key: SecretStr | None = None
    gemini_model_id: str = "gemini-3.5-flash-lite"
    investigation_allow_paid_models: bool = False
    investigation_request_timeout_seconds: float = Field(default=30.0, gt=0, le=120)
    investigation_max_retries: int = Field(default=2, ge=0, le=5)
    investigation_max_llm_calls: int = Field(default=4, gt=0, le=10)
    investigation_max_iterations: int = Field(default=2, gt=0, le=5)
    investigation_evidence_limit: int = Field(default=50, gt=0, le=100)
    investigation_poll_interval_seconds: float = Field(default=2.0, gt=0, le=60)

    @property
    def active_investigation_model_id(self) -> str:
        if self.llm_provider == "gemini":
            return self.gemini_model_id
        return self.investigation_model_id

    @property
    def active_investigation_model_is_free(self) -> bool:
        if self.llm_provider == "gemini":
            return self.gemini_model_id in GEMINI_FREE_MODEL_IDS
        return self.investigation_model_id.endswith(":free")


@lru_cache
def get_settings() -> Settings:
    return Settings()
