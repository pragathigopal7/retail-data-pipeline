-- Line items exploded from each order's first known event.
CREATE OR REPLACE TABLE silver_order_items AS
SELECT
    o.order_id,
    o.store_id,
    o.current_status,
    CAST(COALESCE(o.placed_at, o.first_event_at) AS DATE) AS order_date,
    item ->> 'sku'                                         AS sku,
    item ->> 'category'                                    AS category,
    CAST(item ->> 'quantity' AS INTEGER)                   AS quantity,
    CAST(item ->> 'unit_price' AS DECIMAL(10, 2))          AS unit_price,
    CAST(item ->> 'quantity' AS INTEGER)
        * CAST(item ->> 'unit_price' AS DECIMAL(10, 2))    AS line_total
FROM silver_orders o,
     UNNEST(CAST(o.items_json AS JSON[])) AS t(item);
