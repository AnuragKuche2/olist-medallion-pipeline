# src/gold/build.py
"""
Gold Layer — Star Schema Builder (PySpark).

Transforms Silver → Gold with a dimensional model:
  - Fact tables: fact_orders, fact_order_items
  - Dimension tables: dim_customer, dim_product, dim_seller, dim_date, dim_geography

Design choices:
  - Star schema (not snowflake): fewer joins, small dims, BI-optimized
  - Surrogate keys: deterministic xxhash64 of each dimension's natural key
    (stable across rebuilds; previously monotonically_increasing_id)
  - Pre-aggregations in fact tables for faster queries
  - Conformed dimensions: reusable across fact tables

Phase 2: Migrate to dbt on Databricks with proper refs, tests, and lineage.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from pyspark.sql.types import IntegerType, DoubleType, BooleanType
import os
from datetime import datetime

from src.utils.spark_session import get_spark_session
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================
S3_BUCKET = os.environ.get("S3_BUCKET", "anukuche-olist-datalake")
# Support synthetic data: set SILVER_FOLDER / GOLD_FOLDER env vars
SILVER_FOLDER = os.environ.get("SILVER_FOLDER", "silver")
GOLD_FOLDER = os.environ.get("GOLD_FOLDER", "gold")
SILVER_PATH = f"s3a://{S3_BUCKET}/{SILVER_FOLDER}"
GOLD_PATH = f"s3a://{S3_BUCKET}/{GOLD_FOLDER}"


# ============================================================
# PURE HELPERS (no I/O — unit-testable)
# ============================================================
def seller_tier_expr(orders_col: str = "total_orders"):
    """Return the Column expression that classifies a seller into a tier."""
    return (
        F.when(F.col(orders_col) >= 100, "platinum")
         .when(F.col(orders_col) >= 50, "gold")
         .when(F.col(orders_col) >= 10, "silver")
         .otherwise("bronze")
    )


def add_date_attributes(df: DataFrame, date_col: str = "full_date") -> DataFrame:
    """Add all date-dimension attribute columns derived from `date_col`."""
    return (
        df
        .withColumn("date_key", F.date_format(date_col, "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", F.year(date_col))
        .withColumn("quarter", F.quarter(date_col))
        .withColumn("month", F.month(date_col))
        .withColumn("month_name", F.date_format(date_col, "MMMM"))
        .withColumn("day_of_month", F.dayofmonth(date_col))
        .withColumn("day_of_week", F.dayofweek(date_col))
        .withColumn("day_name", F.date_format(date_col, "EEEE"))
        .withColumn("week_of_year", F.weekofyear(date_col))
        .withColumn("is_weekend", F.when(F.dayofweek(date_col).isin(1, 7), True).otherwise(False))
        .withColumn("year_month", F.date_format(date_col, "yyyy-MM"))
    )


def aggregate_order_payments(payments: DataFrame) -> DataFrame:
    """Aggregate payment rows to one row per order."""
    return (
        payments
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.first("payment_type").alias("primary_payment_type"),
            F.max("payment_installments").alias("max_installments"),
        )
    )


def aggregate_order_items(items: DataFrame) -> DataFrame:
    """Aggregate order-item rows to one row per order."""
    return (
        items
        .groupBy("order_id")
        .agg(
            F.sum("price").alias("total_item_value"),
            F.sum("freight_value").alias("total_freight_value"),
            F.count("*").alias("item_count"),
        )
    )


# ============================================================
# DIMENSION TABLES
# ============================================================

def build_dim_date(spark: SparkSession) -> int:
    """
    Build date dimension from order timestamps.
    Covers full date range in the dataset (2016-09 to 2018-10).
    """
    logger.info("Building: dim_date")

    # Read orders to get date range
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")

    # Get min/max dates
    date_range = orders.select(
        F.min(F.to_date("order_purchase_timestamp")).alias("min_date"),
        F.max(F.to_date("order_purchase_timestamp")).alias("max_date")
    ).collect()[0]

    # Generate date spine
    df = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{date_range.min_date}'),
            to_date('{date_range.max_date}'),
            interval 1 day
        )) as full_date
    """)

    # Build date attributes
    df = add_date_attributes(df, "full_date")

    df.cache()
    count = df.count()
    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_date")
    df.unpersist()
    logger.info(f"Written {count:,} records ({date_range.min_date} to {date_range.max_date})")
    return count


def build_dim_customer(spark: SparkSession) -> int:
    """
    Build customer dimension.
    Grain: one row per customer_unique_id (not customer_id).
    """
    logger.info("Building: dim_customer")

    customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")

    # A customer_unique_id can have multiple customer_ids (one per order).
    # Keep the row associated with their MOST RECENT order so the address
    # reflects where they last shipped to (important for geo analytics).
    customer_orders = customers.join(
        orders.select("customer_id", "order_purchase_timestamp"),
        "customer_id",
        "left"
    )

    window = Window.partitionBy("customer_unique_id").orderBy(
        F.col("order_purchase_timestamp").desc_nulls_last()
    )
    df = (
        customer_orders
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "order_purchase_timestamp", "customer_id")
    )

    df = (
        df
        # Deterministic surrogate key: stable across rebuilds (hash of the
        # natural key) instead of monotonically_increasing_id(), which changed
        # every run and would break incremental loads / SCD / BI bookmarks.
        .withColumn("customer_key", F.xxhash64(F.col("customer_unique_id")))
        .select(
            "customer_key",
            "customer_unique_id",
            F.col("customer_city").alias("city"),
            F.col("customer_state").alias("state"),
            "region",
            F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        )
    )

    df.cache()
    count = df.count()
    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_customer")
    df.unpersist()
    logger.info(f"Written {count:,} records")
    return count


def build_dim_product(spark: SparkSession) -> int:
    """
    Build product dimension with English category names.
    Joins products with category translation.
    """
    logger.info("Building: dim_product")

    products = spark.read.format("delta").load(f"{SILVER_PATH}/products")
    categories = spark.read.format("delta").load(f"{SILVER_PATH}/category_translation")

    # Join to get English category names
    df = products.join(
        categories,
        products.product_category_name == categories.product_category_name,
        "left"
    ).drop(categories.product_category_name)

    df = (
        df
        # Deterministic surrogate key (stable across rebuilds)
        .withColumn("product_key", F.xxhash64(F.col("product_id")))
        .select(
            "product_key",
            "product_id",
            F.col("product_category_name").alias("category_pt"),
            F.col("product_category_name_english").alias("category_en"),
            "product_weight_kg",
            "product_volume_cm3",
            F.col("product_name_lenght").alias("name_length"),
            F.col("product_description_lenght").alias("description_length"),
            "product_photos_qty",
        )
    )

    df.cache()
    count = df.count()
    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_product")
    df.unpersist()
    logger.info(f"Written {count:,} records")
    return count


def build_dim_seller(spark: SparkSession) -> int:
    """
    Build seller dimension with performance metrics.
    Enriches with average review score and order count.
    """
    logger.info("Building: dim_seller")

    sellers = spark.read.format("delta").load(f"{SILVER_PATH}/sellers")
    order_items = spark.read.format("delta").load(f"{SILVER_PATH}/order_items")
    reviews = spark.read.format("delta").load(f"{SILVER_PATH}/order_reviews")
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")

    # Calculate seller metrics: order count + avg review score
    seller_orders = (
        order_items
        .groupBy("seller_id")
        .agg(F.countDistinct("order_id").alias("total_orders"))
    )

    # Get review scores per seller (through order_items → orders → reviews)
    seller_reviews = (
        order_items
        .select("seller_id", "order_id")
        .join(reviews.select("order_id", "review_score"), "order_id", "inner")
        .groupBy("seller_id")
        .agg(F.avg("review_score").alias("avg_review_score"))
    )

    # Join everything
    df = (
        sellers
        .join(seller_orders, "seller_id", "left")
        .join(seller_reviews, "seller_id", "left")
    )

    # Add seller tier based on order volume
    df = df.withColumn("seller_tier", seller_tier_expr("total_orders"))

    df = (
        df
        # Deterministic surrogate key (stable across rebuilds)
        .withColumn("seller_key", F.xxhash64(F.col("seller_id")))
        .select(
            "seller_key",
            "seller_id",
            F.col("seller_city").alias("city"),
            F.col("seller_state").alias("state"),
            "region",
            F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
            "total_orders",
            F.round("avg_review_score", 2).alias("avg_review_score"),
            "seller_tier",
        )
    )

    df.cache()
    count = df.count()
    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_seller")
    df.unpersist()
    logger.info(f"Written {count:,} records")
    return count


def build_dim_geography(spark: SparkSession) -> int:
    """
    Build geography dimension from geolocation data.
    Grain: one row per zip_code_prefix with avg coordinates.
    """
    logger.info("Building: dim_geography")

    geo = spark.read.format("delta").load(f"{SILVER_PATH}/geolocation")

    df = (
        geo
        .withColumn("geography_key", F.xxhash64(F.col("geolocation_zip_code_prefix")))
        .select(
            "geography_key",
            F.col("geolocation_zip_code_prefix").alias("zip_code_prefix"),
            F.col("geolocation_lat").alias("latitude"),
            F.col("geolocation_lng").alias("longitude"),
            F.col("geolocation_city").alias("city"),
            F.col("geolocation_state").alias("state"),
        )
    )

    df.cache()
    count = df.count()
    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_geography")
    df.unpersist()
    logger.info(f"Written {count:,} records")
    return count


# ============================================================
# FACT TABLES
# ============================================================

def build_fact_orders(spark: SparkSession) -> int:
    """
    Build fact_orders: one row per order, enriched with payment/item aggregates
    and dimension keys.
    """
    logger.info("Building: fact_orders")

    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")
    payments = spark.read.format("delta").load(f"{SILVER_PATH}/order_payments")
    items = spark.read.format("delta").load(f"{SILVER_PATH}/order_items")

    # Aggregate payments and items to order grain
    payment_agg = aggregate_order_payments(payments)
    item_agg = aggregate_order_items(items)

    # Get customer_unique_id for the surrogate key lookup
    customer_lookup = customers.select("customer_id", "customer_unique_id").dropDuplicates(["customer_id"])

    # Build fact table
    df = (
        orders
        .join(customer_lookup, "customer_id", "left")
        .join(payment_agg, "order_id", "left")
        .join(item_agg, "order_id", "left")
    )

    # Add dimension keys
    df = (
        df
        .withColumn("date_key", F.date_format(F.to_date("order_purchase_timestamp"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("customer_key", F.xxhash64(F.col("customer_unique_id")))
        .select(
            "order_id",
            "date_key",
            "customer_key",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "delivery_days",
            "is_late_delivery",
            "total_payment_value",
            "primary_payment_type",
            "max_installments",
            "total_item_value",
            "total_freight_value",
            "item_count",
        )
    )

    df.cache()
    count = df.count()
    df.write.format("delta").mode("overwrite").partitionBy("date_key").save(f"{GOLD_PATH}/fact_orders")
    df.unpersist()
    logger.info(f"Written {count:,} records")
    return count


def build_fact_order_items(spark: SparkSession) -> int:
    """
    Build fact_order_items: one row per order line item with dimension keys.
    Grain: (order_id, order_item_id).
    """
    logger.info("Building: fact_order_items")

    items = spark.read.format("delta").load(f"{SILVER_PATH}/order_items")
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")

    # Get date from orders for the date key
    order_dates = orders.select(
        "order_id",
        F.date_format(F.to_date("order_purchase_timestamp"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
    )

    df = (
        items
        .join(order_dates, "order_id", "left")
        .withColumn("product_key", F.xxhash64(F.col("product_id")))
        .withColumn("seller_key", F.xxhash64(F.col("seller_id")))
        .select(
            "order_id",
            "order_item_id",
            "date_key",
            "product_key",
            "seller_key",
            "price",
            "freight_value",
            "shipping_limit_date",
        )
    )

    df.cache()
    count = df.count()
    df.write.format("delta").mode("overwrite").partitionBy("date_key").save(f"{GOLD_PATH}/fact_order_items")
    df.unpersist()
    logger.info(f"Written {count:,} records")
    return count


# ============================================================
# BUILD ALL
# ============================================================
def build_all():
    """Build the full Gold star schema."""
    logger.info("=" * 60)
    logger.info("GOLD LAYER — Star Schema Build")
    logger.info("=" * 60)

    spark = get_spark_session(app_name="Gold_Build")

    # Dimensions first (facts reference them)
    results = {}
    results["dim_date"] = build_dim_date(spark)
    results["dim_customer"] = build_dim_customer(spark)
    results["dim_product"] = build_dim_product(spark)
    results["dim_seller"] = build_dim_seller(spark)
    results["dim_geography"] = build_dim_geography(spark)

    # Facts
    results["fact_orders"] = build_fact_orders(spark)
    results["fact_order_items"] = build_fact_order_items(spark)

    # Summary
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    for table, count in results.items():
        logger.info(f"  {table:25s} → {count:>10,} records")
    logger.info(f"  {'TOTAL':25s} → {sum(results.values()):>10,} records")
    logger.info("=" * 60)

    # Clean shutdown — release Spark resources
    spark.stop()

    return results


if __name__ == "__main__":
    build_all()
