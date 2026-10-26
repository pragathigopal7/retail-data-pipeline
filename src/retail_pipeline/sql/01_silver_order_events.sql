-- materialized: append
-- unique_key: event_id
--
-- Deduplicated event log. Kafka delivery is at least once, so the same
-- event_id can arrive more than once, within a run or across runs; keep the
-- first copy we ingested. Only bronze rows above the watermark are read.
SELECT * EXCLUDE (rn)
FROM (
    SELECT
        b.*,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at, source_offset) AS rn
    FROM new_bronze b
)
WHERE rn = 1
