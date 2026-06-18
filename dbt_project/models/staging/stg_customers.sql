-- models/staging/stg_customers.sql

{{ config(materialized='view') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    region
FROM delta.`s3a://anukuche-olist-datalake/silver/customers`
