-- models/staging/stg_sellers.sql

{{ config(materialized='view') }}

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    region
FROM delta.`s3a://anukuche-olist-datalake/silver/sellers`
