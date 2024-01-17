from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class TelemetryEvent(BaseModel):
    vehicle_id: str = Field(min_length=3, max_length=64)
    event_time: datetime
    engine_temp_c: float
    rpm: int
    battery_voltage: float
    oil_pressure_kpa: float
    speed_kph: float = Field(ge=0)
    gps_lat: float = Field(ge=-90, le=90)
    gps_lon: float = Field(ge=-180, le=180)
    odometer_km: float = Field(ge=0)
    fault_codes: list[str] = Field(default_factory=list)

    @field_validator("vehicle_id")
    @classmethod
    def normalize_vehicle_id(cls, value: str) -> str:
        return value.upper().strip()

    @field_validator("event_time")
    @classmethod
    def make_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_ranges(self) -> "TelemetryEvent":
        if self.engine_temp_c < -40 or self.engine_temp_c > 220:
            raise ValueError("engine_temp_c outside realistic operating range")
        if self.rpm < 0 or self.rpm > 8000:
            raise ValueError("rpm outside realistic operating range")
        if self.battery_voltage < 0 or self.battery_voltage > 30:
            raise ValueError("battery_voltage outside realistic operating range")
        if self.oil_pressure_kpa < 0 or self.oil_pressure_kpa > 1000:
            raise ValueError("oil_pressure_kpa outside realistic operating range")
        return self


class EnrichedTelemetryEvent(TelemetryEvent):
    ingestion_time: datetime
    ingestion_date: str
    temperature_alert: bool
    battery_alert: bool
    oil_alert: bool
    fault_count: int
    engine_load_band: str
    health_hint: str


class PredictionRecord(BaseModel):
    vehicle_id: str
    event_time: datetime
    risk_score: float = Field(ge=0, le=1)
    risk_band: str
    fault_count: int
    temperature_alert: bool
    battery_alert: bool
    oil_alert: bool


class QuarantineRecord(BaseModel):
    received_at: datetime
    payload: dict[str, Any]
    errors: list[str]
