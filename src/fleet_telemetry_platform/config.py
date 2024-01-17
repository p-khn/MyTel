from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "dev"
    log_level: str = "INFO"

    broker_backend: str = "local"
    broker_topic_raw: str = "telemetry.raw"
    local_broker_root: Path = Path("./data/broker")
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = "fleet-telemetry-consumer"

    lakehouse_root: Path = Path("./data/lakehouse")
    duckdb_path: Path = Path("./data/lakehouse/warehouse.duckdb")

    quality_max_engine_temp_c: float = 140.0
    quality_min_battery_voltage: float = 9.0
    quality_min_oil_pressure_kpa: float = 80.0

    api_host: str = "0.0.0.0"
    api_port: int = 8080


settings = Settings()
