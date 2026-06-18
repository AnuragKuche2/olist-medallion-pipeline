-- models/staging/stg_order_items.sql

{{ config(materialized='view') }}

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
FROM delta.`s3a://anukuche-olist-datalake/silver/order_items`
