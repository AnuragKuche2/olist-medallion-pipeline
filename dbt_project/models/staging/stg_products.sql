-- models/staging/stg_products.sql

{{ config(materialized='view') }}

SELECT
    product_id,
    product_category_name,
    product_name_lenght AS product_name_length,
    product_description_lenght AS product_description_length,
    product_photos_qty,
    product_weight_kg,
    product_volume_cm3
FROM delta.`s3a://anukuche-olist-datalake/silver/products`
