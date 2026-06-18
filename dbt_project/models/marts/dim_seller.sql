-- models/marts/dim_seller.sql
-- Seller dimension with performance metrics and tier classification

{{ config(materialized='table') }}

WITH seller_orders AS (
    SELECT
        seller_id,
        COUNT(DISTINCT order_id) AS total_orders
    FROM {{ ref('stg_order_items') }}
    GROUP BY seller_id
),

seller_reviews AS (
    SELECT
        oi.seller_id,
        AVG(r.review_score) AS avg_review_score
    FROM {{ ref('stg_order_items') }} oi
    INNER JOIN {{ ref('stg_order_reviews') }} r ON oi.order_id = r.order_id
    GROUP BY oi.seller_id
)

SELECT
    monotonically_increasing_id() AS seller_key,
    s.seller_id,
    s.seller_city AS city,
    s.seller_state AS state,
    s.region,
    s.seller_zip_code_prefix AS zip_code_prefix,
    COALESCE(so.total_orders, 0) AS total_orders,
    ROUND(sr.avg_review_score, 2) AS avg_review_score,
    CASE
        WHEN so.total_orders >= 100 THEN 'platinum'
        WHEN so.total_orders >= 50 THEN 'gold'
        WHEN so.total_orders >= 10 THEN 'silver'
        ELSE 'bronze'
    END AS seller_tier
FROM {{ ref('stg_sellers') }} s
LEFT JOIN seller_orders so ON s.seller_id = so.seller_id
LEFT JOIN seller_reviews sr ON s.seller_id = sr.seller_id
