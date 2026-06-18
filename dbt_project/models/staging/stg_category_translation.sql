-- models/staging/stg_category_translation.sql

{{ config(materialized='view') }}

SELECT
    product_category_name,
    product_category_name_english
FROM delta.`s3a://anukuche-olist-datalake/silver/category_translation`
