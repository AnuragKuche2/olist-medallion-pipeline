-- models/staging/stg_order_payments.sql

{{ config(materialized='view') }}

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM delta.`s3a://anukuche-olist-datalake/silver/order_payments`
