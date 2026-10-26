-- materialized: incremental
-- unique_key: order_date
--
-- Payment method mix per day and channel, from schema v2 onward. Orders that
-- only ever arrived as v1 events have no payment method and are reported as
-- 'unknown' rather than dropped, so totals still reconcile with daily sales.
SELECT
    CAST(COALESCE(placed_at, first_event_at) AS DATE)                    AS order_date,
    channel,
    COALESCE(payment_method, 'unknown')                                  AS payment_method,
    COUNT(*)                                                             AS orders,
    SUM(order_total - discount_total)
        FILTER (WHERE current_status NOT IN ('CANCELLED', 'REFUNDED'))   AS net_revenue
FROM silver_orders
WHERE CAST(COALESCE(placed_at, first_event_at) AS DATE)
      IN (SELECT order_date FROM changed_order_date)
GROUP BY ALL
ORDER BY order_date, channel, payment_method
