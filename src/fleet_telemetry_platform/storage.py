from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

try:
    import duckdb  # type: ignore
except ImportError:  # pragma: no cover - environment dependent
    duckdb = None

try:
    import polars as pl  # type: ignore
except ImportError:  # pragma: no cover - environment dependent
    pl = None

from .config import settings
from .models import EnrichedTelemetryEvent, PredictionRecord, QuarantineRecord


class LakehouseStore:
    def __init__(self, root: Path | None = None, duckdb_path: Path | None = None):
        self.root = root or settings.lakehouse_root
        self.duckdb_path = duckdb_path or settings.duckdb_path
        self.bronze_root = self.root / "bronze"
        self.silver_root = self.root / "silver"
        self.quarantine_root = self.root / "quarantine"
        self.gold_root = self.root / "gold"
        for path in [self.bronze_root, self.silver_root, self.quarantine_root, self.gold_root]:
            path.mkdir(parents=True, exist_ok=True)
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        self._backend = "duckdb" if duckdb is not None else "sqlite"
        self._init_db()

    def _connect(self):
        if self._backend == "duckdb":
            return duckdb.connect(str(self.duckdb_path))
        conn = sqlite3.connect(str(self.duckdb_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_events (
                    vehicle_id TEXT,
                    event_time TEXT,
                    engine_temp_c REAL,
                    rpm INTEGER,
                    battery_voltage REAL,
                    oil_pressure_kpa REAL,
                    speed_kph REAL,
                    gps_lat REAL,
                    gps_lon REAL,
                    odometer_km REAL,
                    fault_codes TEXT,
                    ingestion_time TEXT,
                    ingestion_date TEXT,
                    temperature_alert INTEGER,
                    battery_alert INTEGER,
                    oil_alert INTEGER,
                    fault_count INTEGER,
                    engine_load_band TEXT,
                    health_hint TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gold_predictions (
                    vehicle_id TEXT,
                    event_time TEXT,
                    risk_score REAL,
                    risk_band TEXT,
                    fault_count INTEGER,
                    temperature_alert INTEGER,
                    battery_alert INTEGER,
                    oil_alert INTEGER,
                    created_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS quarantine_records (
                    received_at TEXT,
                    payload TEXT,
                    errors TEXT
                )
                """
            )
            conn.commit()

    def append_bronze(self, raw_messages: Iterable[dict]) -> Path:
        day = datetime.now(timezone.utc).date().isoformat()
        partition = self.bronze_root / f"ingestion_date={day}"
        partition.mkdir(parents=True, exist_ok=True)
        output = partition / f"batch_{uuid.uuid4().hex}.jsonl"
        with output.open("w", encoding="utf-8") as handle:
            for message in raw_messages:
                handle.write(json.dumps(message) + "\n")
        return output

    def append_silver(self, records: list[EnrichedTelemetryEvent]) -> Path:
        if not records:
            raise ValueError("append_silver requires at least one record")
        rows = [self._event_to_row(record) for record in records]
        day = records[0].ingestion_date
        if pl is not None:
            partition = self.silver_root / f"ingestion_date={day}"
            partition.mkdir(parents=True, exist_ok=True)
            output = partition / f"batch_{uuid.uuid4().hex}.parquet"
            pl.DataFrame(rows).write_parquet(output)
        else:
            partition = self.silver_root / f"ingestion_date={day}"
            partition.mkdir(parents=True, exist_ok=True)
            output = partition / f"batch_{uuid.uuid4().hex}.jsonl"
            self._write_jsonl(output, rows)

        with self._connect() as conn:
            if self._backend == "duckdb" and pl is not None:
                frame = pl.DataFrame(rows)
                conn.register("silver_batch_arrow", frame.to_arrow())
                conn.execute("INSERT INTO silver_events SELECT * FROM silver_batch_arrow")
                conn.unregister("silver_batch_arrow")
            else:
                conn.executemany(
                    """
                    INSERT INTO silver_events VALUES (
                        :vehicle_id, :event_time, :engine_temp_c, :rpm, :battery_voltage,
                        :oil_pressure_kpa, :speed_kph, :gps_lat, :gps_lon, :odometer_km,
                        :fault_codes, :ingestion_time, :ingestion_date, :temperature_alert,
                        :battery_alert, :oil_alert, :fault_count, :engine_load_band, :health_hint
                    )
                    """,
                    rows,
                )
                conn.commit()
        return output

    def append_predictions(self, predictions: list[PredictionRecord]) -> Path:
        rows = [
            {**self._prediction_to_row(prediction), "created_at": self._iso(datetime.now(timezone.utc))}
            for prediction in predictions
        ]
        if pl is not None:
            output = self.gold_root / f"predictions_{uuid.uuid4().hex}.parquet"
            pl.DataFrame(rows).write_parquet(output)
        else:
            output = self.gold_root / f"predictions_{uuid.uuid4().hex}.jsonl"
            self._write_jsonl(output, rows)

        with self._connect() as conn:
            if self._backend == "duckdb" and pl is not None:
                frame = pl.DataFrame(rows)
                conn.register("prediction_batch_arrow", frame.to_arrow())
                conn.execute("INSERT INTO gold_predictions SELECT * FROM prediction_batch_arrow")
                conn.unregister("prediction_batch_arrow")
            else:
                conn.executemany(
                    """
                    INSERT INTO gold_predictions VALUES (
                        :vehicle_id, :event_time, :risk_score, :risk_band, :fault_count,
                        :temperature_alert, :battery_alert, :oil_alert, :created_at
                    )
                    """,
                    rows,
                )
                conn.commit()
        return output

    def append_quarantine(self, records: list[QuarantineRecord]) -> Path:
        rows = [
            {
                "received_at": self._iso(record.received_at),
                "payload": json.dumps(record.payload),
                "errors": " | ".join(record.errors),
            }
            for record in records
        ]
        if pl is not None:
            output = self.quarantine_root / f"quarantine_{uuid.uuid4().hex}.parquet"
            pl.DataFrame(rows).write_parquet(output)
        else:
            output = self.quarantine_root / f"quarantine_{uuid.uuid4().hex}.jsonl"
            self._write_jsonl(output, rows)

        with self._connect() as conn:
            if self._backend == "duckdb" and pl is not None:
                frame = pl.DataFrame(rows)
                conn.register("quarantine_batch_arrow", frame.to_arrow())
                conn.execute("INSERT INTO quarantine_records SELECT * FROM quarantine_batch_arrow")
                conn.unregister("quarantine_batch_arrow")
            else:
                conn.executemany(
                    "INSERT INTO quarantine_records VALUES (:received_at, :payload, :errors)",
                    rows,
                )
                conn.commit()
        return output

    def latest_predictions(self, limit: int = 20) -> list[dict]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT vehicle_id, event_time, risk_score, risk_band, fault_count,
                       temperature_alert, battery_alert, oil_alert, created_at
                FROM gold_predictions
                ORDER BY created_at DESC
                LIMIT ?
                """,
                [limit],
            )
            rows = cursor.fetchall()
            if self._backend == "duckdb":
                columns = [column[0] for column in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
            return [dict(row) for row in rows]

    def quality_summary(self) -> dict:
        with self._connect() as conn:
            counts = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM silver_events) AS silver_event_count,
                    (SELECT COUNT(*) FROM gold_predictions) AS prediction_count,
                    (SELECT COUNT(*) FROM quarantine_records) AS quarantine_count
                """
            ).fetchone()
        silver_count, prediction_count, quarantine_count = counts
        total = silver_count + quarantine_count
        quarantine_ratio = round(quarantine_count / total, 4) if total else 0.0
        return {
            "silver_event_count": silver_count,
            "prediction_count": prediction_count,
            "quarantine_count": quarantine_count,
            "quarantine_ratio": quarantine_ratio,
            "storage_backend": self._backend,
        }

    @staticmethod
    def _write_jsonl(path: Path, rows: list[dict]) -> None:
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")

    @staticmethod
    def _iso(value):
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value

    @classmethod
    def _event_to_row(cls, event: EnrichedTelemetryEvent) -> dict:
        row = event.model_dump()
        row["event_time"] = cls._iso(row["event_time"])
        row["ingestion_time"] = cls._iso(row["ingestion_time"])
        row["fault_codes"] = json.dumps(row["fault_codes"])
        row["temperature_alert"] = int(bool(row["temperature_alert"]))
        row["battery_alert"] = int(bool(row["battery_alert"]))
        row["oil_alert"] = int(bool(row["oil_alert"]))
        return row

    @classmethod
    def _prediction_to_row(cls, prediction: PredictionRecord) -> dict:
        row = prediction.model_dump()
        row["event_time"] = cls._iso(row["event_time"])
        row["temperature_alert"] = int(bool(row["temperature_alert"]))
        row["battery_alert"] = int(bool(row["battery_alert"]))
        row["oil_alert"] = int(bool(row["oil_alert"]))
        return row
