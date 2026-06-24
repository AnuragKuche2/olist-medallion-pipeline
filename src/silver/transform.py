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

Design choice: each table has a PURE `clean_<table>(df) -> df` function that
contains all transformation logic with no I/O, plus a thin `transform_<table>`
wrapper that reads Bronze, calls the pure function, adds Silver metadata, and
writes. The split keeps the logic unit-testable (tests call clean_* with small
in-memory DataFrames) while the wrappers handle storage.
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
S3_BUCKET = os.environ.get("S3_BUCKET", "anukuche-olist-datalake")
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
# SHARED UTILITIES (pure)
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


def _write_silver(df: DataFrame, table: str) -> int:
    """Add Silver metadata, write to Delta, and return the row count."""
    df = _add_silver_metadata(df)
    df.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/{table}")
    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


# ============================================================
# PER-TABLE TRANSFORMS (pure clean_* + thin transform_* wrapper)
# ============================================================

def clean_orders(df: DataFrame) -> DataFrame:
    """Pure Silver transform for orders.

    Dedup on order_id, cast timestamp columns, derive delivery_days and
    is_late_delivery, and standardize order_status.
    """
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)
    df = df.dropDuplicates(["order_id"])

    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col_name in timestamp_cols:
        df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))

    df = df.withColumn(
        "delivery_days",
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_purchase_timestamp"),
        ),
    )
    df = df.withColumn(
        "is_late_delivery",
        F.when(
            F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
    df = df.withColumn("order_status", F.lower(F.col("order_status")))
    return df


def transform_orders(spark: SparkSession) -> int:
    """Read Bronze orders → clean → write Silver."""
    print("\n🔄 Transforming: orders")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")
    return _write_silver(clean_orders(df), "orders")


def clean_order_items(df: DataFrame) -> DataFrame:
    """Pure Silver transform for order_items.

    Dedup on (order_id, order_item_id), cast numeric/timestamp columns,
    and flag invalid prices.
    """
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)
    df = df.dropDuplicates(["order_id", "order_item_id"])

    df = df.withColumn("order_item_id", F.col("order_item_id").cast(IntegerType()))
    df = df.withColumn("price", F.col("price").cast(DoubleType()))
    df = df.withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
    df = df.withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))

    df = df.withColumn(
        "_dq_valid_price",
        F.when(F.col("price") > 0, F.lit(True)).otherwise(F.lit(False)),
    )
    return df


def transform_order_items(spark: SparkSession) -> int:
    """Read Bronze order_items → clean → write Silver."""
    print("\n🔄 Transforming: order_items")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/order_items")
    return _write_silver(clean_order_items(df), "order_items")


def clean_order_payments(df: DataFrame) -> DataFrame:
    """Pure Silver transform for order_payments.

    Dedup on (order_id, payment_sequential), cast numerics, standardize type.
    """
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)
    df = df.dropDuplicates(["order_id", "payment_sequential"])

    df = df.withColumn("payment_sequential", F.col("payment_sequential").cast(IntegerType()))
    df = df.withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
    df = df.withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
    df = df.withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
    return df


def transform_order_payments(spark: SparkSession) -> int:
    """Read Bronze order_payments → clean → write Silver."""
    print("\n🔄 Transforming: order_payments")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/order_payments")
    return _write_silver(clean_order_payments(df), "order_payments")


def clean_order_reviews(df: DataFrame) -> DataFrame:
    """Pure Silver transform for order_reviews.

    Dedup on review_id, cast score/timestamps, convert empty comments to null.
    """
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)
    df = df.dropDuplicates(["review_id"])

    df = df.withColumn("review_score", F.col("review_score").cast(IntegerType()))
    df = df.withColumn("review_creation_date", F.to_timestamp(F.col("review_creation_date")))
    df = df.withColumn("review_answer_timestamp", F.to_timestamp(F.col("review_answer_timestamp")))

    df = df.withColumn(
        "review_comment_title",
        F.when(F.col("review_comment_title") == "", F.lit(None)).otherwise(F.col("review_comment_title")),
    )
    df = df.withColumn(
        "review_comment_message",
        F.when(F.col("review_comment_message") == "", F.lit(None)).otherwise(F.col("review_comment_message")),
    )
    return df


def transform_order_reviews(spark: SparkSession) -> int:
    """Read Bronze order_reviews → clean → write Silver."""
    print("\n🔄 Transforming: order_reviews")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/order_reviews")
    return _write_silver(clean_order_reviews(df), "order_reviews")


def clean_customers(df: DataFrame) -> DataFrame:
    """Pure Silver transform for customers.

    Dedup on customer_id, standardize city/state, add region, pad zip.
    """
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)
    df = df.dropDuplicates(["customer_id"])

    df = _standardize_city(df, "customer_city")
    df = df.withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    df = _add_region(df, "customer_state")
    df = df.withColumn(
        "customer_zip_code_prefix",
        F.lpad(F.col("customer_zip_code_prefix"), 5, "0"),
    )
    return df


def transform_customers(spark: SparkSession) -> int:
    """Read Bronze customers → clean → write Silver."""
    print("\n🔄 Transforming: customers")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/customers")
    return _write_silver(clean_customers(df), "customers")


def clean_products(df: DataFrame) -> DataFrame:
    """Pure Silver transform for products.

    Dedup on product_id, cast dimensions, derive weight_kg and volume_cm3,
    fill null category with 'unknown'.
    """
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)
    df = df.dropDuplicates(["product_id"])

    numeric_cols = [
        "product_name_lenght", "product_description_lenght",
        "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm",
    ]
    for col_name in numeric_cols:
        df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    df = df.withColumn("product_weight_kg", F.col("product_weight_g") / 1000.0)
    df = df.withColumn(
        "product_volume_cm3",
        F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"),
    )
    df = df.withColumn(
        "product_category_name",
        F.coalesce(F.col("product_category_name"), F.lit("unknown")),
    )
    return df


def transform_products(spark: SparkSession) -> int:
    """Read Bronze products → clean → write Silver."""
    print("\n🔄 Transforming: products")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/products")
    return _write_silver(clean_products(df), "products")


def clean_sellers(df: DataFrame) -> DataFrame:
    """Pure Silver transform for sellers.

    Dedup on seller_id, standardize city/state, add region, pad zip.
    """
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)
    df = df.dropDuplicates(["seller_id"])

    df = _standardize_city(df, "seller_city")
    df = df.withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    df = _add_region(df, "seller_state")
    df = df.withColumn(
        "seller_zip_code_prefix",
        F.lpad(F.col("seller_zip_code_prefix"), 5, "0"),
    )
    return df


def transform_sellers(spark: SparkSession) -> int:
    """Read Bronze sellers → clean → write Silver."""
    print("\n🔄 Transforming: sellers")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/sellers")
    return _write_silver(clean_sellers(df), "sellers")


def clean_geolocation(df: DataFrame) -> DataFrame:
    """Pure Silver transform for geolocation.

    Cast coordinates, standardize city/state, pad zip, then aggregate to one
    row per zip prefix (avg lat/lng, first city/state) and add region.
    """
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)

    df = df.withColumn("geolocation_lat", F.col("geolocation_lat").cast(DoubleType()))
    df = df.withColumn("geolocation_lng", F.col("geolocation_lng").cast(DoubleType()))
    df = _standardize_city(df, "geolocation_city")
    df = df.withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))
    df = df.withColumn(
        "geolocation_zip_code_prefix",
        F.lpad(F.col("geolocation_zip_code_prefix"), 5, "0"),
    )

    df = df.groupBy("geolocation_zip_code_prefix").agg(
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng"),
        F.first("geolocation_city").alias("geolocation_city"),
        F.first("geolocation_state").alias("geolocation_state"),
    )
    df = _add_region(df, "geolocation_state")
    return df


def transform_geolocation(spark: SparkSession) -> int:
    """Read Bronze geolocation → clean (aggregate to zip) → write Silver."""
    print("\n🔄 Transforming: geolocation")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/geolocation")
    return _write_silver(clean_geolocation(df), "geolocation")


def clean_category_translation(df: DataFrame) -> DataFrame:
    """Pure Silver transform for category_translation.

    Dedup on product_category_name and lowercase both columns.
    """
    df = _drop_bronze_metadata(df)
    df = _trim_strings(df)
    df = df.dropDuplicates(["product_category_name"])

    df = df.withColumn("product_category_name", F.lower(F.col("product_category_name")))
    df = df.withColumn("product_category_name_english", F.lower(F.col("product_category_name_english")))
    return df


def transform_category_translation(spark: SparkSession) -> int:
    """Read Bronze category_translation → clean → write Silver."""
    print("\n🔄 Transforming: category_translation")
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/category_translation")
    return _write_silver(clean_category_translation(df), "category_translation")


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
