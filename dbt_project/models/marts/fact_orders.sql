-- models/marts/fact_orders.sql
-- Fact table — one row per order
-- Pre-aggregates payments, items, and reviews to order grain

{{ config(materialized='table') }}

WITH order_payments AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment_value,
        FIRST(payment_type) AS primary_payment_type,
        MAX(payment_installments) AS max_installments
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),

order_items_agg AS (
    SELECT
        order_id,
        SUM(price) AS total_item_value,
        SUM(freight_value) AS total_freight_value,
        COUNT(*) AS item_count
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),

order_reviews AS (
    SELECT
        order_id,
        review_score
    FROM {{ ref('stg_order_reviews') }}
),

customer_lookup AS (
    SELECT
        customer_id,
        customer_unique_id
    FROM {{ ref('stg_customers') }}
)

SELECT
    o.order_id,
    dc.customer_key,
    CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS date_key,
    o.order_status,
    oia.total_item_value,
    oia.total_freight_value,
    op.total_payment_value,
    op.primary_payment_type,
    op.max_installments,
    oia.item_count,
    orv.review_score,
    o.delivery_days,
    o.is_late_delivery,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date
FROM {{ ref('stg_orders') }} o
LEFT JOIN customer_lookup cl ON o.customer_id = cl.customer_id
LEFT JOIN {{ ref('dim_customer') }} dc ON cl.customer_unique_id = dc.customer_unique_id
LEFT JOIN order_payments op ON o.order_id = op.order_id
LEFT JOIN order_items_agg oia ON o.order_id = oia.order_id
LEFT JOIN order_reviews orv ON o.order_id = orv.order_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.order_id) = 1
