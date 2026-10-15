-- Daily sales by store and channel. Net revenue excludes cancelled and
-- refunded orders.
CREATE OR REPLACE TABLE gold_daily_store_sales AS
SELECT
    CAST(COALESCE(placed_at, first_event_at) AS DATE)                    AS order_date,
    store_id,
    channel,
    COUNT(*)                                                             AS orders,
    SUM(order_total)                                                     AS gross_revenue,
    SUM(order_total) FILTER (WHERE current_status NOT IN ('CANCELLED', 'REFUNDED'))
                                                                         AS net_revenue,
    ROUND(AVG(order_total), 2)                                           AS avg_order_value,
    COUNT(*) FILTER (WHERE current_status = 'CANCELLED')                 AS cancelled_orders,
    COUNT(*) FILTER (WHERE current_status = 'REFUNDED')                  AS refunded_orders
FROM silver_orders
GROUP BY ALL
ORDER BY order_date, store_id, channel;
