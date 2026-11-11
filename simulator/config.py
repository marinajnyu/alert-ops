from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SimulatorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SIMULATOR_", env_file=".env", extra="ignore")
    database_url: str = "postgresql+psycopg://simulator:simulator@localhost:5433/simulator"
    # Stable account names map to credentials; account identity must not change on restart.
    accounts: dict[str, SecretStr]

    @model_validator(mode="after")
    def valid_accounts(self):
        values = [value.get_secret_value() for value in self.accounts.values()]
        if not values or any(not value for value in values) or len(set(values)) != len(values):
            raise ValueError("Configure nonempty, distinct simulator account credentials")
        if any(not name or len(name) > 128 for name in self.accounts):
            raise ValueError("Invalid simulator account name")
        return self
