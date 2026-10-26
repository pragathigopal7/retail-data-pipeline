-- materialized: incremental
-- unique_key: order_date
--
-- Daily sales by store and channel. Net revenue excludes cancelled and
-- refunded orders and is after discounts. Only days touched by changed orders
-- are rebuilt.
SELECT
    CAST(COALESCE(placed_at, first_event_at) AS DATE)                    AS order_date,
    store_id,
    channel,
    COUNT(*)                                                             AS orders,
    SUM(order_total)                                                     AS gross_revenue,
    SUM(discount_total)                                                  AS discounts,
    SUM(order_total - discount_total)
        FILTER (WHERE current_status NOT IN ('CANCELLED', 'REFUNDED'))   AS net_revenue,
    ROUND(AVG(order_total), 2)                                           AS avg_order_value,
    COUNT(*) FILTER (WHERE current_status = 'CANCELLED')                 AS cancelled_orders,
    COUNT(*) FILTER (WHERE current_status = 'REFUNDED')                  AS refunded_orders
FROM silver_orders
WHERE CAST(COALESCE(placed_at, first_event_at) AS DATE)
      IN (SELECT order_date FROM changed_order_date)
GROUP BY ALL
ORDER BY order_date, store_id, channel
