from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from .domain import enrich_event, score_event
from .metrics import BATCH_DURATION, EVENTS_PROCESSED, EVENTS_QUARANTINED, PREDICTIONS_WRITTEN
from .models import QuarantineRecord, TelemetryEvent
from .quality import QualityError, run_quality_checks
from .storage import LakehouseStore

logger = logging.getLogger(__name__)


class TelemetryPipeline:
    def __init__(self, store: LakehouseStore):
        self.store = store

    def process_batch(self, payloads: list[dict[str, Any]]) -> dict[str, int]:
        with BATCH_DURATION.time():
            self.store.append_bronze(payloads)
            good_records = []
            bad_records = []
            predictions = []

            for payload in payloads:
                try:
                    event = TelemetryEvent.model_validate(payload)
                    run_quality_checks(event)
                    enriched = enrich_event(event)
                    prediction = score_event(enriched)
                    good_records.append(enriched)
                    predictions.append(prediction)
                except (ValidationError, QualityError, ValueError) as exc:
                    bad_records.append(
                        QuarantineRecord(
                            received_at=datetime.now(timezone.utc),
                            payload=payload,
                            errors=[str(exc)],
                        )
                    )

            if good_records:
                self.store.append_silver(good_records)
                self.store.append_predictions(predictions)
                EVENTS_PROCESSED.inc(len(good_records))
                PREDICTIONS_WRITTEN.inc(len(predictions))

            if bad_records:
                self.store.append_quarantine(bad_records)
                EVENTS_QUARANTINED.inc(len(bad_records))
                logger.warning("quarantined %s records", len(bad_records))

            return {
                "processed": len(good_records),
                "quarantined": len(bad_records),
            }
