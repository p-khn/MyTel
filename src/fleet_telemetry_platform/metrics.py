from prometheus_client import Counter, Histogram

EVENTS_PROCESSED = Counter(
    "fleet_events_processed_total",
    "Number of successfully processed telemetry events",
)

EVENTS_QUARANTINED = Counter(
    "fleet_events_quarantined_total",
    "Number of quarantined telemetry events",
)

PREDICTIONS_WRITTEN = Counter(
    "fleet_predictions_written_total",
    "Number of risk predictions written",
)

BATCH_DURATION = Histogram(
    "fleet_batch_duration_seconds",
    "Duration spent processing a batch",
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 2, 5, 8),
)
