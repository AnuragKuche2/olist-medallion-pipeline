-- models/marts/dim_date.sql
-- Date dimension — generated from order timestamps

{{ config(materialized='table') }}

WITH date_range AS (
    SELECT
        MIN(CAST(order_purchase_timestamp AS DATE)) AS min_date,
        MAX(CAST(order_purchase_timestamp AS DATE)) AS max_date
    FROM {{ ref('stg_orders') }}
),

date_spine AS (
    SELECT explode(sequence(min_date, max_date, interval 1 day)) AS full_date
    FROM date_range
)

SELECT
    CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    year(full_date) AS year,
    quarter(full_date) AS quarter,
    month(full_date) AS month,
    date_format(full_date, 'MMMM') AS month_name,
    dayofmonth(full_date) AS day_of_month,
    dayofweek(full_date) AS day_of_week,
    date_format(full_date, 'EEEE') AS day_name,
    weekofyear(full_date) AS week_of_year,
    CASE WHEN dayofweek(full_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    date_format(full_date, 'yyyy-MM') AS year_month
FROM date_spine
