-- Top vehicles by average risk score
SELECT
    vehicle_id,
    ROUND(AVG(risk_score), 3) AS avg_risk_score,
    COUNT(*) AS scored_events
FROM gold_predictions
GROUP BY vehicle_id
ORDER BY avg_risk_score DESC
LIMIT 10;

-- Quarantine ratio
WITH totals AS (
    SELECT (SELECT COUNT(*) FROM gold_predictions) AS good_records,
           (SELECT COUNT(*) FROM quarantine_records) AS quarantined_records
)
SELECT *,
       ROUND(quarantined_records::DOUBLE / NULLIF(good_records + quarantined_records, 0), 4) AS quarantine_ratio
FROM totals;

-- Latest high-risk events
SELECT *
FROM gold_predictions
WHERE risk_band = 'high'
ORDER BY event_time DESC
LIMIT 50;
