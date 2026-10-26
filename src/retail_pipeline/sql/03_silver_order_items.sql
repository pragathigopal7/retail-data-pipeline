-- materialized: incremental
-- unique_key: order_id
--
-- Line items exploded from each order's first known event. v1 items carry no
-- discount, so it defaults to zero.
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
        * CAST(item ->> 'unit_price' AS DECIMAL(10, 2))    AS line_total,
    COALESCE(CAST(item ->> 'discount' AS DECIMAL(10, 2)), 0) AS discount
FROM silver_orders o,
     UNNEST(CAST(o.items_json AS JSON[])) AS t(item)
WHERE o.order_id IN (SELECT order_id FROM changed_order_id)
