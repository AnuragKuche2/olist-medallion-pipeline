-- models/staging/stg_geolocation.sql

{{ config(materialized='view') }}

SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    region
FROM delta.`s3a://anukuche-olist-datalake/silver/geolocation`
