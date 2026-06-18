-- models/marts/dim_geography.sql
-- Geography dimension — one row per zip prefix

{{ config(materialized='table') }}

SELECT
    monotonically_increasing_id() AS geo_key,
    geolocation_zip_code_prefix AS zip_code_prefix,
    geolocation_city AS city,
    geolocation_state AS state,
    region,
    ROUND(geolocation_lat, 6) AS latitude,
    ROUND(geolocation_lng, 6) AS longitude
FROM {{ ref('stg_geolocation') }}
