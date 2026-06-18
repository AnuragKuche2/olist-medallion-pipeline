-- models/marts/dim_product.sql
-- Product dimension with English category names

{{ config(materialized='table') }}

SELECT
    monotonically_increasing_id() AS product_key,
    p.product_id,
    p.product_category_name AS category_pt,
    c.product_category_name_english AS category_en,
    p.product_weight_kg,
    p.product_volume_cm3,
    p.product_name_length AS name_length,
    p.product_description_length AS description_length,
    p.product_photos_qty
FROM {{ ref('stg_products') }} p
LEFT JOIN {{ ref('stg_category_translation') }} c
    ON p.product_category_name = c.product_category_name
