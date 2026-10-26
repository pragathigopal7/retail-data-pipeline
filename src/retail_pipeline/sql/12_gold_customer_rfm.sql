-- materialized: table
--
-- Recency, frequency, monetary segmentation as of the latest event in the data.
-- Net of discounts. NTILE ties are broken by customer_id so scores are stable.
WITH as_of AS (SELECT MAX(last_event_at) AS ts FROM silver_orders),
base AS (
    SELECT
        customer_id,
        DATE_DIFF('day', MAX(COALESCE(placed_at, first_event_at)), (SELECT ts FROM as_of)) AS recency_days,
        COUNT(*)                                                                         AS frequency,
        SUM(order_total - discount_total)                                                AS monetary
    FROM silver_orders
    WHERE current_status NOT IN ('CANCELLED', 'REFUNDED')
    GROUP BY customer_id
),
scored AS (
    SELECT
        *,
        6 - NTILE(5) OVER (ORDER BY recency_days, customer_id) AS r_score,
        NTILE(5) OVER (ORDER BY frequency, customer_id)   AS f_score,
        NTILE(5) OVER (ORDER BY monetary, customer_id)    AS m_score
    FROM base
)
SELECT
    *,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 THEN 'champion'
        WHEN r_score >= 4                  THEN 'recent'
        WHEN f_score >= 4                  THEN 'loyal_lapsing'
        WHEN r_score <= 2                  THEN 'at_risk'
        ELSE 'regular'
    END AS segment
FROM scored
