# src/gold/build.py
"""
Gold Layer — Star Schema Builder (PySpark).

Transforms Silver → Gold with a dimensional model:
  - Fact tables: fact_orders, fact_order_items
  - Dimension tables: dim_customer, dim_product, dim_seller, dim_date, dim_geography

Design choices:
  - Star schema (not snowflake): fewer joins, small dims, BI-optimized
  - Surrogate keys: monotonically_increasing_id for dim tables
  - Pre-aggregations in fact tables for faster queries
  - Conformed dimensions: reusable across fact tables

Phase 2: Migrate to dbt on Databricks with proper refs, tests, and lineage.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from pyspark.sql.types import IntegerType, DoubleType, BooleanType
from datetime import datetime

from src.utils.spark_session import get_spark_session


# ============================================================
# CONFIGURATION
# ============================================================
S3_BUCKET = "anukuche-olist-datalake"
SILVER_PATH = f"s3a://{S3_BUCKET}/silver"
GOLD_PATH = f"s3a://{S3_BUCKET}/gold"


# ============================================================
# DIMENSION TABLES
# ============================================================

def build_dim_date(spark: SparkSession) -> int:
    """
    Build date dimension from order timestamps.
    Covers full date range in the dataset (2016-09 to 2018-10).
    """
    print("\n📐 Building: dim_date")

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
    df = (
        df
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("is_weekend", F.when(F.dayofweek("full_date").isin(1, 7), True).otherwise(False))
        .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_date")

    count = df.count()
    print(f"   ✅ Written: {count:,} records ({date_range.min_date} to {date_range.max_date})")
    return count


def build_dim_customer(spark: SparkSession) -> int:
    """
    Build customer dimension.
    Grain: one row per customer_unique_id (not customer_id).
    """
    print("\n📐 Building: dim_customer")

    customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")

    # Deduplicate to unique customers (a customer can have multiple customer_ids)
    df = customers.dropDuplicates(["customer_unique_id"])

    df = (
        df
        .withColumn("customer_key", F.monotonically_increasing_id())
        .select(
            "customer_key",
            "customer_unique_id",
            F.col("customer_city").alias("city"),
            F.col("customer_state").alias("state"),
            "region",
            F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        )
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_customer")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def build_dim_product(spark: SparkSession) -> int:
    """
    Build product dimension with English category names.
    Joins products with category translation.
    """
    print("\n📐 Building: dim_product")

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
        .withColumn("product_key", F.monotonically_increasing_id())
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

    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_product")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def build_dim_seller(spark: SparkSession) -> int:
    """
    Build seller dimension with performance metrics.
    Enriches with average review score and order count.
    """
    print("\n📐 Building: dim_seller")

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
    df = df.withColumn(
        "seller_tier",
        F.when(F.col("total_orders") >= 100, "platinum")
         .when(F.col("total_orders") >= 50, "gold")
         .when(F.col("total_orders") >= 10, "silver")
         .otherwise("bronze")
    )

    df = (
        df
        .withColumn("seller_key", F.monotonically_increasing_id())
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

    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_seller")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def build_dim_geography(spark: SparkSession) -> int:
    """
    Build geography dimension from geolocation.
    Grain: one row per zip_code_prefix.
    """
    print("\n📐 Building: dim_geography")

    geo = spark.read.format("delta").load(f"{SILVER_PATH}/geolocation")

    df = (
        geo
        .withColumn("geo_key", F.monotonically_increasing_id())
        .select(
            "geo_key",
            F.col("geolocation_zip_code_prefix").alias("zip_code_prefix"),
            F.col("geolocation_city").alias("city"),
            F.col("geolocation_state").alias("state"),
            "region",
            F.round("geolocation_lat", 6).alias("latitude"),
            F.round("geolocation_lng", 6).alias("longitude"),
        )
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_geography")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


# ============================================================
# FACT TABLES
# ============================================================

def build_fact_orders(spark: SparkSession) -> int:
    """
    Build fact_orders — one row per order.
    Pre-aggregates payment and item-level info to order grain.
    Includes delivery metrics and review scores.
    """
    print("\n📊 Building: fact_orders")

    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    payments = spark.read.format("delta").load(f"{SILVER_PATH}/order_payments")
    items = spark.read.format("delta").load(f"{SILVER_PATH}/order_items")
    reviews = spark.read.format("delta").load(f"{SILVER_PATH}/order_reviews")
    customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")

    # Load dim_customer for surrogate key lookup
    dim_customer = spark.read.format("delta").load(f"{GOLD_PATH}/dim_customer")

    # Aggregate payments to order level
    order_payments = (
        payments
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.first("payment_type").alias("primary_payment_type"),
            F.max("payment_installments").alias("max_installments"),
        )
    )

    # Aggregate items to order level
    order_items_agg = (
        items
        .groupBy("order_id")
        .agg(
            F.sum("price").alias("total_item_value"),
            F.sum("freight_value").alias("total_freight_value"),
            F.count("*").alias("item_count"),
        )
    )

    # Get review score per order
    order_reviews = reviews.select("order_id", "review_score")

    # Get customer_unique_id for key lookup
    customer_lookup = customers.select("customer_id", "customer_unique_id")

    # Build fact table
    df = (
        orders
        .join(customer_lookup, "customer_id", "left")
        .join(dim_customer.select("customer_key", "customer_unique_id"), "customer_unique_id", "left")
        .join(order_payments, "order_id", "left")
        .join(order_items_agg, "order_id", "left")
        .join(order_reviews, "order_id", "left")
    )

    # Deduplicate — customer join can produce multiple rows per order
    df = df.dropDuplicates(["order_id"])

    # Add date_key for date dimension join
    df = df.withColumn("date_key", F.date_format("order_purchase_timestamp", "yyyyMMdd").cast(IntegerType()))

    # Select final columns
    df = df.select(
        "order_id",
        "customer_key",
        "date_key",
        "order_status",
        "total_item_value",
        "total_freight_value",
        "total_payment_value",
        "primary_payment_type",
        "max_installments",
        "item_count",
        "review_score",
        "delivery_days",
        "is_late_delivery",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/fact_orders")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


def build_fact_order_items(spark: SparkSession) -> int:
    """
    Build fact_order_items — one row per order line item.
    Grain: (order_id, order_item_id) — more granular than fact_orders.
    Links to product and seller dimensions.
    """
    print("\n📊 Building: fact_order_items")

    items = spark.read.format("delta").load(f"{SILVER_PATH}/order_items")
    orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")

    # Load dims for surrogate key lookups
    dim_product = spark.read.format("delta").load(f"{GOLD_PATH}/dim_product")
    dim_seller = spark.read.format("delta").load(f"{GOLD_PATH}/dim_seller")

    # Join items with order date for date_key
    df = items.join(
        orders.select("order_id", "order_purchase_timestamp"),
        "order_id",
        "left"
    )

    # Lookup surrogate keys
    df = (
        df
        .join(dim_product.select("product_key", "product_id"), "product_id", "left")
        .join(dim_seller.select("seller_key", "seller_id"), "seller_id", "left")
    )

    # Add date_key
    df = df.withColumn("date_key", F.date_format("order_purchase_timestamp", "yyyyMMdd").cast(IntegerType()))

    # Select final columns
    df = df.select(
        "order_id",
        "order_item_id",
        "product_key",
        "seller_key",
        "date_key",
        "price",
        "freight_value",
        "shipping_limit_date",
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/fact_order_items")

    count = df.count()
    print(f"   ✅ Written: {count:,} records")
    return count


# ============================================================
# BUILD ALL
# ============================================================
def build_all():
    """Build complete Gold star schema."""

    print("=" * 60)
    print("GOLD LAYER — Star Schema Build")
    print("=" * 60)

    spark = get_spark_session(app_name="Gold_Build")

    results = {}

    # Build dimensions FIRST (fact tables reference them)
    print("\n" + "-" * 40)
    print("DIMENSIONS")
    print("-" * 40)

    dims = [
        ("dim_date", build_dim_date),
        ("dim_customer", build_dim_customer),
        ("dim_product", build_dim_product),
        ("dim_seller", build_dim_seller),
        ("dim_geography", build_dim_geography),
    ]

    for name, build_fn in dims:
        count = build_fn(spark)
        results[name] = count

    # Build facts AFTER dimensions
    print("\n" + "-" * 40)
    print("FACTS")
    print("-" * 40)

    facts = [
        ("fact_orders", build_fact_orders),
        ("fact_order_items", build_fact_order_items),
    ]

    for name, build_fn in facts:
        count = build_fn(spark)
        results[name] = count

    # --- Summary ---
    print("\n" + "=" * 60)
    print("GOLD LAYER SUMMARY")
    print("=" * 60)
    print("\n   Dimensions:")
    for table, count in results.items():
        if table.startswith("dim_"):
            print(f"      {table:25s} → {count:>10,} records")
    print("\n   Facts:")
    for table, count in results.items():
        if table.startswith("fact_"):
            print(f"      {table:25s} → {count:>10,} records")
    print(f"\n   {'TOTAL':28s} → {sum(results.values()):>10,} records")
    print("=" * 60)

    return results


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    build_all()
