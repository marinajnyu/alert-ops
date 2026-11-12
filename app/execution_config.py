from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ExecutionSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ALERTOPS_", env_file=".env", extra="ignore")
    database_url: str = "postgresql+psycopg://alertops:alertops@localhost:5432/alertops"
    broker_url: str = "redis://127.0.0.1:6379/0"
    task_queue: str = "alertops.fulfillment"
    provider_url: str = "http://127.0.0.1:8001"
    provider_keys: dict[str, SecretStr] = Field(default_factory=dict)
    lease_seconds: int = Field(default=60, ge=15)
    recovery_seconds: int = Field(default=30, ge=1)
    verification_seconds: int = Field(default=30, ge=1)
