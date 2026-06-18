-- models/marts/fact_order_items.sql
-- Fact table — one row per order line item
-- Links to product and seller dimensions via surrogate keys

{{ config(materialized='table') }}

SELECT
    oi.order_id,
    oi.order_item_id,
    dp.product_key,
    ds.seller_key,
    CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS date_key,
    oi.price,
    oi.freight_value,
    oi.shipping_limit_date
FROM {{ ref('stg_order_items') }} oi
LEFT JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
LEFT JOIN {{ ref('dim_product') }} dp ON oi.product_id = dp.product_id
LEFT JOIN {{ ref('dim_seller') }} ds ON oi.seller_id = ds.seller_id
