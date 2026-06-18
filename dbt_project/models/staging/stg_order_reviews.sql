-- models/staging/stg_order_reviews.sql

{{ config(materialized='view') }}

SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM delta.`s3a://anukuche-olist-datalake/silver/order_reviews`
