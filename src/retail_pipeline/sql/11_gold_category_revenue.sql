CREATE OR REPLACE TABLE gold_category_revenue AS
SELECT
    order_date,
    category,
    SUM(quantity)                AS units,
    SUM(line_total)              AS gross_revenue,
    COUNT(DISTINCT order_id)     AS orders
FROM silver_order_items
WHERE current_status NOT IN ('CANCELLED', 'REFUNDED')
GROUP BY ALL
ORDER BY order_date, gross_revenue DESC;
