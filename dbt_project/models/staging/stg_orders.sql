-- models/staging/stg_orders.sql
-- Thin wrapper over Silver orders — minimal transformation
-- Just renaming and type safety for downstream refs

{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    delivery_days,
    is_late_delivery
FROM delta.`s3a://anukuche-olist-datalake/silver/orders`
