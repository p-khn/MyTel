from __future__ import annotations

import logging
import time

from ..broker.factory import get_broker
from ..config import settings
from ..pipeline import TelemetryPipeline
from ..storage import LakehouseStore

logger = logging.getLogger(__name__)


class TelemetryConsumer:
    def __init__(self, store: LakehouseStore | None = None):
        self.broker = get_broker()
        self.store = store or LakehouseStore()
        self.pipeline = TelemetryPipeline(self.store)

    def run(self, max_batches: int = 10, batch_size: int = 100, poll_seconds: float = 1.0) -> dict[str, int]:
        total_processed = 0
        total_quarantined = 0

        for batch_number in range(1, max_batches + 1):
            messages = self.broker.consume(settings.broker_topic_raw, max_messages=batch_size)
            if not messages:
                logger.info("batch %s: no messages available", batch_number)
                time.sleep(poll_seconds)
                continue

            payloads = [message.payload for message in messages]
            result = self.pipeline.process_batch(payloads)
            for message in messages:
                message.ack()

            total_processed += result["processed"]
            total_quarantined += result["quarantined"]
            logger.info(
                "batch %s complete | processed=%s quarantined=%s",
                batch_number,
                result["processed"],
                result["quarantined"],
            )

        return {
            "processed": total_processed,
            "quarantined": total_quarantined,
        }
