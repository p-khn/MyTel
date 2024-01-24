from __future__ import annotations

from datetime import datetime, timezone

from .models import EnrichedTelemetryEvent, PredictionRecord, TelemetryEvent


def enrich_event(event: TelemetryEvent) -> EnrichedTelemetryEvent:
    temperature_alert = event.engine_temp_c >= 105
    battery_alert = event.battery_voltage < 12.0
    oil_alert = event.oil_pressure_kpa < 130.0

    if event.rpm >= 2200:
        engine_load_band = "high"
    elif event.rpm >= 1200:
        engine_load_band = "medium"
    else:
        engine_load_band = "low"

    active_alerts = sum([temperature_alert, battery_alert, oil_alert])
    if active_alerts >= 2 or len(event.fault_codes) >= 2:
        health_hint = "investigate"
    elif active_alerts == 1 or len(event.fault_codes) == 1:
        health_hint = "watch"
    else:
        health_hint = "normal"

    return EnrichedTelemetryEvent(
        **event.model_dump(),
        ingestion_time=datetime.now(timezone.utc),
        ingestion_date=event.event_time.astimezone(timezone.utc).date().isoformat(),
        temperature_alert=temperature_alert,
        battery_alert=battery_alert,
        oil_alert=oil_alert,
        fault_count=len(event.fault_codes),
        engine_load_band=engine_load_band,
        health_hint=health_hint,
    )


def scoring_event(event: EnrichedTelemetryEvent) -> PredictionRecord:
    score = 0.15
    score += 0.30 if event.temperature_alert else 0.0
    score += 0.25 if event.battery_alert else 0.0
    score += 0.20 if event.oil_alert else 0.0
    score += min(0.10 * event.fault_count, 0.30)
    score += 0.05 if event.engine_load_band == "high" else 0.0
    score = min(round(score, 4), 1.0)

    if score >= 0.75:
        band = "high"
    elif score >= 0.45:
        band = "medium"
    else:
        band = "low"

    return PredictionRecord(
        vehicle_id=event.vehicle_id,
        event_time=event.event_time,
        risk_score=score,
        risk_band=band,
        fault_count=event.fault_count,
        temperature_alert=event.temperature_alert,
        battery_alert=event.battery_alert,
        oil_alert=event.oil_alert,
    )
