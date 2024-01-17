from __future__ import annotations

from .config import settings
from .models import TelemetryEvent


class QualityError(ValueError):
    pass


def run_quality_checks(event: TelemetryEvent) -> None:
    errors: list[str] = []
    if event.engine_temp_c > settings.quality_max_engine_temp_c:
        errors.append(
            f"engine_temp_c {event.engine_temp_c} exceeds configured max {settings.quality_max_engine_temp_c}"
        )
    if event.battery_voltage < settings.quality_min_battery_voltage:
        errors.append(
            f"battery_voltage {event.battery_voltage} below configured min {settings.quality_min_battery_voltage}"
        )
    if event.oil_pressure_kpa < settings.quality_min_oil_pressure_kpa:
        errors.append(
            f"oil_pressure_kpa {event.oil_pressure_kpa} below configured min {settings.quality_min_oil_pressure_kpa}"
        )

    if errors:
        raise QualityError("; ".join(errors))
