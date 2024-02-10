from datetime import UTC, datetime

from fleet_telemetry_platform.domain import enrich_event, score_event
from fleet_telemetry_platform.models import TelemetryEvent


def test_scoring_marks_high_risk_when_multiple_alerts_present():
    event = TelemetryEvent(
        vehicle_id="bus-014",
        event_time=datetime(2026, 3, 29, 8, 16, tzinfo=UTC),
        engine_temp_c=110.0,
        rpm=2550,
        battery_voltage=11.4,
        oil_pressure_kpa=100.0,
        speed_kph=32.0,
        gps_lat=40.21,
        gps_lon=-8.42,
        odometer_km=150000.0,
        fault_codes=["P0217", "P0524"],
    )

    enriched = enrich_event(event)
    prediction = score_event(enriched)

    assert enriched.health_hint == "investigate"
    assert prediction.risk_band == "high"
    assert prediction.risk_score >= 0.75
