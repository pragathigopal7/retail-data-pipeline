-- Deduplicated event log. Kafka delivery is at least once, so the same
-- event_id can arrive more than once; keep the first copy we ingested.
CREATE OR REPLACE TABLE silver_order_events AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT
        b.*,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at, source_offset) AS rn
    FROM bronze_order_events b
)
WHERE rn = 1;
