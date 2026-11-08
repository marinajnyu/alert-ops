from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ALERTOPS_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://alertops:alertops@localhost:5432/alertops"
    webhook_secret: SecretStr
    max_body_bytes: int = Field(default=65536, gt=0)
    requests_per_minute: int = Field(default=120, gt=0)
