from pathlib import Path

from fleet_telemetry_platform.pipeline import TelemetryPipeline
from fleet_telemetry_platform.storage import LakehouseStore


def test_pipeline_processes_good_records_and_quarantines_bad_ones(tmp_path: Path):
    store = LakehouseStore(root=tmp_path / "lakehouse", duckdb_path=tmp_path / "warehouse.duckdb")
    pipeline = TelemetryPipeline(store)

    payloads = [
        {
            "vehicle_id": "BUS-001",
            "event_time": "2026-03-29T08:15:00Z",
            "engine_temp_c": 95.0,
            "rpm": 1800,
            "battery_voltage": 12.6,
            "oil_pressure_kpa": 210.0,
            "speed_kph": 40.0,
            "gps_lat": 40.20,
            "gps_lon": -8.42,
            "odometer_km": 1000.0,
            "fault_codes": [],
        },
        {
            "vehicle_id": "BUS-002",
            "event_time": "2026-03-29T08:16:00Z",
            "engine_temp_c": 150.0,
            "rpm": 1800,
            "battery_voltage": 8.0,
            "oil_pressure_kpa": 75.0,
            "speed_kph": 40.0,
            "gps_lat": 40.20,
            "gps_lon": -8.42,
            "odometer_km": 1000.0,
            "fault_codes": ["P0217"],
        },
    ]

    result = pipeline.process_batch(payloads)
    summary = store.quality_summary()
    latest = store.latest_predictions(limit=5)

    assert result == {"processed": 1, "quarantined": 1}
    assert summary["silver_event_count"] == 1
    assert summary["quarantine_count"] == 1
    assert len(latest) == 1
    assert latest[0]["vehicle_id"] == "BUS-001"
