-- models/marts/dim_customer.sql
-- Customer dimension — grain: one row per customer_unique_id

{{ config(materialized='table') }}

WITH deduplicated AS (
    SELECT DISTINCT
        customer_unique_id,
        customer_city AS city,
        customer_state AS state,
        region,
        customer_zip_code_prefix AS zip_code_prefix
    FROM {{ ref('stg_customers') }}
)

SELECT
    monotonically_increasing_id() AS customer_key,
    customer_unique_id,
    city,
    state,
    region,
    zip_code_prefix
FROM deduplicated
