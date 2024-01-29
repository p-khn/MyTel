from pathlib import Path

from fastapi.testclient import TestClient

from fleet_telemetry_platform.api import create_app
from fleet_telemetry_platform.pipeline import TelemetryPipeline
from fleet_telemetry_platform.storage import LakehouseStore


def test_api_returns_health_and_predictions(tmp_path: Path):
    store = LakehouseStore(root=tmp_path / "lakehouse", duckdb_path=tmp_path / "warehouse.duckdb")
    pipeline = TelemetryPipeline(store)
    pipeline.process_batch(
        [
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
            }
        ]
    )

    client = TestClient(create_app(store))
    health_response = client.get("/health")
    predictions_response = client.get("/predictions/latest")

    assert health_response.status_code == 200
    assert health_response.json()["silver_event_count"] == 1
    assert predictions_response.status_code == 200
    assert predictions_response.json()[0]["vehicle_id"] == "BUS-001"
