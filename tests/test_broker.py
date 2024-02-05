from pathlib import Path

from fleet_telemetry_platform.broker.local_queue import LocalQueueBroker


def test_local_queue_broker_round_trip(tmp_path: Path):
    broker = LocalQueueBroker(root=tmp_path)
    broker.publish("telemetry.raw", {"vehicle_id": "BUS-001", "rpm": 1200})

    messages = broker.consume("telemetry.raw", max_messages=10)
    assert len(messages) == 1
    assert messages[0].payload["vehicle_id"] == "BUS-001"

    messages[0].ack()
    assert broker.consume("telemetry.raw") == []
