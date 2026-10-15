-- Fulfillment speed per store: hours from placement to delivery.
CREATE OR REPLACE TABLE gold_fulfillment_sla AS
SELECT
    store_id,
    COUNT(*)                                                                         AS delivered_orders,
    ROUND(QUANTILE_CONT(DATE_DIFF('minute', placed_at, delivered_at) / 60.0, 0.5), 1) AS p50_hours,
    ROUND(QUANTILE_CONT(DATE_DIFF('minute', placed_at, delivered_at) / 60.0, 0.9), 1) AS p90_hours,
    ROUND(100.0 * COUNT(*) FILTER (WHERE DATE_DIFF('hour', placed_at, delivered_at) <= 72)
          / COUNT(*), 1)                                                             AS pct_within_72h
FROM silver_orders
WHERE placed_at IS NOT NULL AND delivered_at IS NOT NULL
GROUP BY store_id
ORDER BY store_id;
