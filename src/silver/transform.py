# src/silver/transform.py
"""
Silver Layer Transformation Module.

Transforms Bronze → Silver with:
  - Data type casting (strings → timestamps, proper numerics)
  - Deduplication (remove exact and logical duplicates)
  - Null handling (fill defaults or flag)
  - Standardization (lowercase, trim, consistent formats)
  - Derived columns (delivery_days, is_late, regions)
  - Data quality flags

Design choice: One module with per-table transform functions.
Each function encapsulates table-specific logic while sharing
common utilities. This balances DRY with readability.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    TimestampType, DoubleType, IntegerType, StringType
)
import os
from datetime import datetime

from src.utils.spark_session import get_spark_session


# ============================================================
# CONFIGURATION
# ============================================================
S3_BUCKET = "anukuche-olist-datalake"
# Support synthetic data: set BRONZE_FOLDER / SILVER_FOLDER env vars
BRONZE_FOLDER = os.environ.get("BRONZE_FOLDER", "bronze")
SILVER_FOLDER = os.environ.get("SILVER_FOLDER", "silver")
BRONZE_PATH = f"s3a://{S3_BUCKET}/{BRONZE_FOLDER}"
SILVER_PATH = f"s3a://{S3_BUCKET}/{SILVER_FOLDER}"

# Brazilian state → region mapping
STATE_TO_REGION = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte",
    "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste",
    "MA": "Nordeste", "PB": "Nordeste", "PE": "Nordeste",
    "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste",
    "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}


# ============================================================
# SHARED UTILITIES
# ============================================================
def _trim_strings(df: DataFrame) -> DataFrame:
    """Trim whitespace from all string columns."""
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
    return df


def _standardize_city(df: DataFrame, col_name: str = "city") -> DataFrame:
    """Lowercase and clean city names."""
    if col_name in df.columns:
        df = df.withColumn(col_name, F.lower(F.trim(F.col(col_name))))
        # Remove extra spaces
        df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), r"\s+", " "))
    return df


def _add_region(df: DataFrame, state_col: str = "state") -> DataFrame:
    """Map Brazilian state codes to region names."""
    from pyspark.sql.functions import create_map, lit
    from itertools import chain

    mapping_expr = create_map(
        [lit(x) for x in chain(*STATE_TO_REGION.items())]
    )
    df = df.withColumn("region", mapping_expr[F.col(state_col)])
    return df


def _drop_bronze_metadata(df: DataFrame) -> DataFrame:
    """Remove Bronze-layer metadata columns before Silver write."""
    meta_cols = ["_ingestion_timestamp", "_source_file", "_batch_id"]
    existing = [c for c in meta_cols if c in df.columns]
    return df.drop(*existing)


def _add_silver_metadata(df: DataFrame) -> DataFrame:
    """Add Silver-layer processing metadata."""
    return (
        df
        .withColumn("_silver_processed_at", F.lit(datetime.now().isoformat()))
        .withColumn("_silver_version", F.lit("1.0"))
    )


# ============================================================
# PER-TABLE TRANSFORMS
# ============================================================

def transform_orders(spark: SparkSession) -> int:
    """
    Silver transform for orders.
    - Cast timestamp columns
    - Derive: delivery_days, is_late_delivery
    - Deduplicate on order_id
    """
    print("\n🔄 Transforming: orders")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)

    # Deduplicate
    before = df.count()
    df = df.dropDuplicates(["order_id"])
    after = df.count()
    if before != after:
        print(f"   🧹 Deduped: {before - after} duplicates removed")

    # Cast timestamps
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col_name in timestamp_cols:
        df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))

    # Derived: delivery days (actual)
    df = df.withColumn(
        "delivery_days",
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_purchase_timestamp")
        )
    )

    # Derived: is_late_delivery
    df = df.withColumn(
        "is_late_delivery",
        F.when(
            F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    # Standardize status
    df = df.withColumn("order_status", F.lower(F.col("order_status")))

    df = _add_silver_metadata(df)
    df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/orders")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def transform_order_items(spark: SparkSession) -> int:
    """
    Silver transform for order_items.
    - Cast price/freight to double
    - Deduplicate on (order_id, order_item_id)
    - Validate: price > 0
    """
    print("\n🔄 Transforming: order_items")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/order_items")
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)

    # Deduplicate
    before = df.count()
    df = df.dropDuplicates(["order_id", "order_item_id"])
    after = df.count()
    if before != after:
        print(f"   🧹 Deduped: {before - after} duplicates removed")

    # Cast types
    df = df.withColumn("order_item_id", F.col("order_item_id").cast(IntegerType()))
    df = df.withColumn("price", F.col("price").cast(DoubleType()))
    df = df.withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
    df = df.withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))

    # Data quality: flag invalid prices
    df = df.withColumn(
        "_dq_valid_price",
        F.when(F.col("price") > 0, F.lit(True)).otherwise(F.lit(False))
    )

    df = _add_silver_metadata(df)
    df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/order_items")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def transform_order_payments(spark: SparkSession) -> int:
    """
    Silver transform for order_payments.
    - Cast payment_value to double, payment_installments to int
    - Standardize payment_type
    - Deduplicate on (order_id, payment_sequential)
    """
    print("\n🔄 Transforming: order_payments")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/order_payments")
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)

    # Deduplicate
    before = df.count()
    df = df.dropDuplicates(["order_id", "payment_sequential"])
    after = df.count()
    if before != after:
        print(f"   🧹 Deduped: {before - after} duplicates removed")

    # Cast types
    df = df.withColumn("payment_sequential", F.col("payment_sequential").cast(IntegerType()))
    df = df.withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
    df = df.withColumn("payment_value", F.col("payment_value").cast(DoubleType()))

    # Standardize payment type
    df = df.withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))

    df = _add_silver_metadata(df)
    df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/order_payments")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def transform_order_reviews(spark: SparkSession) -> int:
    """
    Silver transform for order_reviews.
    - Cast review_score to int, timestamps to proper types
    - Deduplicate on review_id (this fixes the 4,938 extras from Bronze)
    - Handle nulls in review_comment_message
    """
    print("\n🔄 Transforming: order_reviews")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/order_reviews")
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)

    # Deduplicate — this is the key fix for the extra rows
    before = df.count()
    df = df.dropDuplicates(["review_id"])
    after = df.count()
    if before != after:
        print(f"   🧹 Deduped: {before - after} duplicates removed")

    # Cast types
    df = df.withColumn("review_score", F.col("review_score").cast(IntegerType()))
    df = df.withColumn("review_creation_date", F.to_timestamp(F.col("review_creation_date")))
    df = df.withColumn("review_answer_timestamp", F.to_timestamp(F.col("review_answer_timestamp")))

    # Handle nulls — empty comments → null (explicit)
    df = df.withColumn(
        "review_comment_title",
        F.when(F.col("review_comment_title") == "", F.lit(None)).otherwise(F.col("review_comment_title"))
    )
    df = df.withColumn(
        "review_comment_message",
        F.when(F.col("review_comment_message") == "", F.lit(None)).otherwise(F.col("review_comment_message"))
    )

    df = _add_silver_metadata(df)
    df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/order_reviews")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def transform_customers(spark: SparkSession) -> int:
    """
    Silver transform for customers.
    - Deduplicate on customer_id
    - Standardize city names (lowercase)
    - Add region column from state
    - Zip code as string (preserve leading zeros)
    """
    print("\n🔄 Transforming: customers")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/customers")
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)

    # Deduplicate
    before = df.count()
    df = df.dropDuplicates(["customer_id"])
    after = df.count()
    if before != after:
        print(f"   🧹 Deduped: {before - after} duplicates removed")

    # Standardize
    df = _standardize_city(df, "customer_city")
    df = df.withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))

    # Add region
    df = _add_region(df, "customer_state")

    # Ensure zip is string (preserve leading zeros for Brazilian CEPs)
    df = df.withColumn(
        "customer_zip_code_prefix",
        F.lpad(F.col("customer_zip_code_prefix"), 5, "0")
    )

    df = _add_silver_metadata(df)
    df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/customers")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def transform_products(spark: SparkSession) -> int:
    """
    Silver transform for products.
    - Cast dimensions to double, weight to double
    - Derive: volume_cm3 (length × height × width)
    - Derive: weight_kg (weight_g / 1000)
    - Deduplicate on product_id
    - Handle null category
    """
    print("\n🔄 Transforming: products")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/products")
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)

    # Deduplicate
    before = df.count()
    df = df.dropDuplicates(["product_id"])
    after = df.count()
    if before != after:
        print(f"   🧹 Deduped: {before - after} duplicates removed")

    # Cast dimensions
    numeric_cols = [
        "product_name_lenght", "product_description_lenght",
        "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm"
    ]
    for col_name in numeric_cols:
        df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    # Derived: weight in kg
    df = df.withColumn("product_weight_kg", F.col("product_weight_g") / 1000.0)

    # Derived: volume in cm³
    df = df.withColumn(
        "product_volume_cm3",
        F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")
    )

    # Handle null category
    df = df.withColumn(
        "product_category_name",
        F.coalesce(F.col("product_category_name"), F.lit("unknown"))
    )

    df = _add_silver_metadata(df)
    df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/products")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def transform_sellers(spark: SparkSession) -> int:
    """
    Silver transform for sellers.
    - Deduplicate on seller_id
    - Standardize city names
    - Add region from state
    - Pad zip code
    """
    print("\n🔄 Transforming: sellers")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/sellers")
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)

    # Deduplicate
    before = df.count()
    df = df.dropDuplicates(["seller_id"])
    after = df.count()
    if before != after:
        print(f"   🧹 Deduped: {before - after} duplicates removed")

    # Standardize
    df = _standardize_city(df, "seller_city")
    df = df.withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))

    # Add region
    df = _add_region(df, "seller_state")

    # Pad zip code
    df = df.withColumn(
        "seller_zip_code_prefix",
        F.lpad(F.col("seller_zip_code_prefix"), 5, "0")
    )

    df = _add_silver_metadata(df)
    df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/sellers")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def transform_geolocation(spark: SparkSession) -> int:
    """
    Silver transform for geolocation.
    - HEAVY dedup (1M → ~19K unique zip prefixes)
    - Cast lat/lng to double
    - Standardize city
    - Add region
    - Aggregate: take first lat/lng per zip prefix (or average)
    """
    print("\n🔄 Transforming: geolocation")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/geolocation")
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)

    # Cast coordinates
    df = df.withColumn("geolocation_lat", F.col("geolocation_lat").cast(DoubleType()))
    df = df.withColumn("geolocation_lng", F.col("geolocation_lng").cast(DoubleType()))

    # Standardize
    df = _standardize_city(df, "geolocation_city")
    df = df.withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))

    # Pad zip code
    df = df.withColumn(
        "geolocation_zip_code_prefix",
        F.lpad(F.col("geolocation_zip_code_prefix"), 5, "0")
    )

    # Deduplicate: aggregate to one row per zip prefix
    # Use average lat/lng and first city/state for each zip
    before = df.count()
    df = df.groupBy("geolocation_zip_code_prefix").agg(
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng"),
        F.first("geolocation_city").alias("geolocation_city"),
        F.first("geolocation_state").alias("geolocation_state"),
    )
    after = df.count()
    print(f"   🧹 Aggregated: {before:,} → {after:,} unique zip prefixes")

    # Add region
    df = _add_region(df, "geolocation_state")

    df = _add_silver_metadata(df)
    df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/geolocation")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def transform_category_translation(spark: SparkSession) -> int:
    """
    Silver transform for category_translation.
    - Trim and lowercase both columns
    - Deduplicate on product_category_name
    """
    print("\n🔄 Transforming: category_translation")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/category_translation")
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)

    # Deduplicate
    before = df.count()
    df = df.dropDuplicates(["product_category_name"])
    after = df.count()
    if before != after:
        print(f"   🧹 Deduped: {before - after} duplicates removed")

    # Standardize
    df = df.withColumn("product_category_name", F.lower(F.col("product_category_name")))
    df = df.withColumn("product_category_name_english", F.lower(F.col("product_category_name_english")))

    df = _add_silver_metadata(df)
    df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/category_translation")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


# ============================================================
# TRANSFORM ALL TABLES
# ============================================================
def transform_all():
    """Run all Silver transformations."""

    print("=" * 60)
    print("SILVER LAYER — Full Transformation")
    print("=" * 60)

    spark = get_spark_session(app_name="Silver_Transform")

    transforms = [
        ("orders", transform_orders),
        ("order_items", transform_order_items),
        ("order_payments", transform_order_payments),
        ("order_reviews", transform_order_reviews),
        ("customers", transform_customers),
        ("products", transform_products),
        ("sellers", transform_sellers),
        ("geolocation", transform_geolocation),
        ("category_translation", transform_category_translation),
    ]

    results = {}
    for table_name, transform_fn in transforms:
        count = transform_fn(spark)
        results[table_name] = count

    # --- Summary ---
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for table, count in results.items():
        print(f"   {table:25s} → {count:>10,} records")
    print(f"\n   {'TOTAL':25s} → {sum(results.values()):>10,} records")
    print("=" * 60)

    return results


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    transform_all()
