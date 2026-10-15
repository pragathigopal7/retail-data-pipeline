-- One row per order with its current state. Current status is decided by
-- event time, not arrival time, so a late PAID event can never overwrite a
-- DELIVERED order. Ties on event_ts fall back to lifecycle rank.
CREATE OR REPLACE TABLE silver_orders AS
WITH ranked AS (
    SELECT
        *,
        CASE status
            WHEN 'PLACED' THEN 1 WHEN 'PAID' THEN 2 WHEN 'SHIPPED' THEN 3
            WHEN 'DELIVERED' THEN 4 WHEN 'CANCELLED' THEN 5 WHEN 'REFUNDED' THEN 6
        END AS status_rank
    FROM silver_order_events
)
SELECT
    order_id,
    ANY_VALUE(customer_id)                                   AS customer_id,
    ANY_VALUE(store_id)                                      AS store_id,
    ANY_VALUE(channel)                                       AS channel,
    ANY_VALUE(currency)                                      AS currency,
    ARG_MAX(status, (event_ts, status_rank))                 AS current_status,
    MIN(event_ts) FILTER (WHERE status = 'PLACED')           AS placed_at,
    MIN(event_ts) FILTER (WHERE status = 'PAID')             AS paid_at,
    MIN(event_ts) FILTER (WHERE status = 'SHIPPED')          AS shipped_at,
    MIN(event_ts) FILTER (WHERE status = 'DELIVERED')        AS delivered_at,
    MIN(event_ts)                                            AS first_event_at,
    MAX(event_ts)                                            AS last_event_at,
    ARG_MIN(order_total, event_ts)                           AS order_total,
    ARG_MIN(items_json, event_ts)                            AS items_json,
    COUNT(*)                                                 AS event_count
FROM ranked
GROUP BY order_id;
