-- materialized: incremental
-- unique_key: order_date
SELECT
    order_date,
    category,
    SUM(quantity)                AS units,
    SUM(line_total)              AS gross_revenue,
    SUM(discount)                AS discounts,
    SUM(line_total - discount)   AS net_revenue,
    COUNT(DISTINCT order_id)     AS orders
FROM silver_order_items
WHERE current_status NOT IN ('CANCELLED', 'REFUNDED')
  AND order_date IN (SELECT order_date FROM changed_order_date)
GROUP BY ALL
ORDER BY order_date, gross_revenue DESC
