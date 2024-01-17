from __future__ import annotations

import json
import logging
import random
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ..broker.factory import get_broker
from ..config import settings

logger = logging.getLogger(__name__)


class TelemetryProducer:
    def __init__(self):
        self.broker = get_broker()

    def publish_file(self, input_path: Path, delay_seconds: float = 0.0) -> int:
        count = 0
        for line in input_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            self.broker.publish(settings.broker_topic_raw, payload)
            count += 1
            if delay_seconds:
                time.sleep(delay_seconds)
        logger.info("published %s records from %s", count, input_path)
        return count

    def publish_generated(self, vehicle_count: int = 10, records: int = 100, delay_seconds: float = 0.0) -> int:
        for event in generate_events(vehicle_count=vehicle_count, records=records):
            self.broker.publish(settings.broker_topic_raw, event)
            if delay_seconds:
                time.sleep(delay_seconds)
        logger.info("published %s generated records", records)
        return records


def generate_events(vehicle_count: int = 10, records: int = 100) -> list[dict]:
    vehicles = [f"BUS-{i:03d}" for i in range(1, vehicle_count + 1)]
    start = datetime.now(UTC) - timedelta(minutes=records)
    rows = []
    for offset in range(records):
        vehicle = random.choice(vehicles)
        overheating = random.random() < 0.08
        low_battery = random.random() < 0.07
        low_oil = random.random() < 0.06
        faults = []
        if overheating:
            faults.append("P0217")
        if low_battery:
            faults.append("P0562")
        if low_oil:
            faults.append("P0524")
        rows.append(
            {
                "vehicle_id": vehicle,
                "event_time": (start + timedelta(minutes=offset)).isoformat().replace("+00:00", "Z"),
                "engine_temp_c": round(random.uniform(82, 99) + (15 if overheating else 0), 2),
                "rpm": random.randint(900, 2600),
                "battery_voltage": round(random.uniform(12.0, 12.8) - (1.2 if low_battery else 0), 2),
                "oil_pressure_kpa": round(random.uniform(160, 240) - (60 if low_oil else 0), 2),
                "speed_kph": round(random.uniform(0, 80), 2),
                "gps_lat": round(40.20 + random.uniform(-0.03, 0.03), 6),
                "gps_lon": round(-8.42 + random.uniform(-0.03, 0.03), 6),
                "odometer_km": round(random.uniform(40000, 260000), 1),
                "fault_codes": faults,
            }
        )
    return rows
