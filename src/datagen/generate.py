# src/datagen/generate.py
"""
Synthetic Data Generator — Phase 2 (1000x Scale).

Generates ~1.55 BILLION records (~150-200GB) maintaining full referential
integrity across all 9 tables in the Olist data model.

Strategy:
  1. Generate base entities first (customers, sellers, products, geolocation)
  2. Generate transactional data referencing base entities (orders, items, payments, reviews)
  3. Use PySpark for generation (not Faker per-row — too slow at this scale)
  4. Write directly to S3 landing zone as CSV (mimics real source system)

Scale factor: 1000x
  - Original: ~100K orders, 1.55M total rows, 140MB
  - Target:   ~100M orders, 1.55B total rows, ~150-200GB

Referential Integrity:
  orders.customer_id → customers.customer_id
  order_items.order_id → orders.order_id
  order_items.product_id → products.product_id
  order_items.seller_id → sellers.seller_id
  order_payments.order_id → orders.order_id
  order_reviews.order_id → orders.order_id
  geolocation.zip_code_prefix → customers/sellers zip prefixes

Usage:
  python3 -m src.datagen.generate --scale 1000
  python3 -m src.datagen.generate --scale 100   # 10x for quick testing
  python3 -m src.datagen.generate --scale 10    # just 10x for dev
"""

import argparse
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, LongType
)
import random

from src.utils.spark_session import get_spark_session


# ============================================================
# CONFIGURATION
# ============================================================
S3_BUCKET = "anukuche-olist-datalake"
OUTPUT_PATH = f"s3a://{S3_BUCKET}/landing_synthetic"

# Base counts (original Olist dataset)
BASE_COUNTS = {
    "customers": 99_441,
    "sellers": 3_095,
    "products": 32_951,
    "geolocation": 19_015,       # unique zip prefixes
    "orders": 99_441,
    "order_items": 112_650,      # ~1.13 items per order avg
    "order_payments": 103_886,   # ~1.04 payments per order avg
    "order_reviews": 99_224,     # ~1 review per order
    "category_translation": 71,  # static — no scaling needed
}

# Brazilian states with approximate population weights
BR_STATES = [
    ("SP", 0.22), ("RJ", 0.08), ("MG", 0.10), ("BA", 0.07),
    ("RS", 0.06), ("PR", 0.06), ("PE", 0.05), ("CE", 0.04),
    ("PA", 0.04), ("MA", 0.03), ("SC", 0.04), ("GO", 0.03),
    ("PB", 0.02), ("AM", 0.02), ("ES", 0.02), ("RN", 0.02),
    ("AL", 0.02), ("PI", 0.02), ("MT", 0.02), ("DF", 0.01),
    ("MS", 0.01), ("SE", 0.01), ("RO", 0.01), ("TO", 0.01),
    ("AC", 0.005), ("AP", 0.005), ("RR", 0.005),
]

ORDER_STATUSES = ["delivered", "shipped", "canceled", "unavailable", "processing"]
STATUS_WEIGHTS = [0.85, 0.05, 0.03, 0.02, 0.05]

PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
PAYMENT_WEIGHTS = [0.70, 0.15, 0.08, 0.07]

PRODUCT_CATEGORIES = [
    "bed_bath_table", "health_beauty", "sports_leisure", "furniture_decor",
    "computers_accessories", "housewares", "watches_gifts", "telephony",
    "garden_tools", "auto", "toys", "cool_stuff", "perfumery",
    "babies", "electronics", "stationery", "fashion_bags_accessories",
    "computers", "home_comfort", "luggage_accessories", "consoles_games",
    "food_drink", "music", "construction_tools_safety", "pet_shop",
    "small_appliances", "agro_industry_and_commerce", "furniture_living_room",
    "signaling_and_security", "office_furniture", "industry_commerce_and_business",
]


# ============================================================
# HELPER FUNCTIONS
# ============================================================
def get_scaled_count(table: str, scale: int) -> int:
    """Get scaled record count for a table."""
    base = BASE_COUNTS[table]
    if table == "category_translation":
        return base  # static — never scale
    return base * scale


def log_progress(table: str, count: int, scale: int):
    """Log generation progress."""
    size_est_mb = count * 0.0001  # rough estimate: 100 bytes per row
    print(f"   📝 {table}: {count:,.0f} records (~{size_est_mb:.0f} MB)")


# Width for zero-padded synthetic IDs. 12 digits covers >900B rows, so it is
# safe at every supported scale and keeps IDs fixed-width for clean joins.
ID_WIDTH = 12


def _make_id(prefix: str, idx_col):
    """Build a deterministic, fixed-width ID column like 'cust_000000000042'.

    Using a deterministic function of a contiguous index is what guarantees
    referential integrity: a child table can reference a parent by reproducing
    the exact same formula over [0, parent_count), so every foreign key is
    certain to exist. This replaces the previous monotonically_increasing_id()
    join, which produced sparse, non-contiguous keys and left most FKs null.
    """
    return F.concat(F.lit(prefix), F.lpad(idx_col.cast(StringType()), ID_WIDTH, "0"))


# ============================================================
# GENERATORS
# ============================================================

def generate_customers(spark: SparkSession, scale: int) -> None:
    """
    Generate synthetic customers.
    Each customer has: customer_id, customer_unique_id, zip, city, state.
    """
    count = get_scaled_count("customers", scale)
    print(f"\n🏭 Generating: customers ({count:,} records)")

    # Generate using Spark — much faster than Faker row-by-row
    df = spark.range(0, count).toDF("row_id")

    # Generate UUIDs for IDs
    df = (
        df
        .withColumn("customer_id", _make_id("cust_", F.col("row_id")))
        .withColumn("customer_unique_id", F.expr("uuid()"))
        # Zip: 5-digit Brazilian CEP (01000-99999)
        .withColumn("customer_zip_code_prefix",
                    F.lpad((F.rand() * 99000 + 1000).cast(IntegerType()).cast(StringType()), 5, "0"))
        # State: weighted random
        .withColumn("state_rand", F.rand())
        .withColumn("customer_state", F.lit("SP"))  # default, will be overridden
        # City: synthetic names
        .withColumn("customer_city",
                    F.concat(F.lit("city_"), (F.rand() * 5000).cast(IntegerType()).cast(StringType())))
    )

    # Assign states based on population weights (cumulative probability)
    cumulative = 0.0
    state_expr = F.lit("SP")  # fallback
    for state, weight in reversed(BR_STATES):
        cumulative_start = sum(w for _, w in BR_STATES[:BR_STATES.index((state, weight))])
        state_expr = F.when(F.col("state_rand") >= cumulative_start, F.lit(state)).otherwise(state_expr)

    df = df.withColumn("customer_state", state_expr).drop("state_rand", "row_id")

    # Write as CSV
    output = f"{OUTPUT_PATH}/olist_customers_dataset.csv"
    df.select("customer_id", "customer_unique_id", "customer_zip_code_prefix",
              "customer_city", "customer_state") \
      .coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite").csv(output)

    log_progress("customers", count, scale)


def generate_sellers(spark: SparkSession, scale: int) -> None:
    """Generate synthetic sellers."""
    count = get_scaled_count("sellers", scale)
    print(f"\n🏭 Generating: sellers ({count:,} records)")

    df = spark.range(0, count).toDF("row_id")

    df = (
        df
        .withColumn("seller_id", _make_id("sell_", F.col("row_id")))
        .withColumn("seller_zip_code_prefix",
                    F.lpad((F.rand() * 99000 + 1000).cast(IntegerType()).cast(StringType()), 5, "0"))
        .withColumn("seller_city",
                    F.concat(F.lit("city_"), (F.rand() * 2000).cast(IntegerType()).cast(StringType())))
        .withColumn("seller_state",
                    F.element_at(
                        F.array([F.lit(s) for s, _ in BR_STATES]),
                        (F.rand() * len(BR_STATES)).cast(IntegerType()) + 1
                    ))
        .drop("row_id")
    )

    output = f"{OUTPUT_PATH}/olist_sellers_dataset.csv"
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output)
    log_progress("sellers", count, scale)


def generate_products(spark: SparkSession, scale: int) -> None:
    """Generate synthetic products with dimensions."""
    count = get_scaled_count("products", scale)
    print(f"\n🏭 Generating: products ({count:,} records)")

    df = spark.range(0, count).toDF("row_id")

    df = (
        df
        .withColumn("product_id", _make_id("prod_", F.col("row_id")))
        .withColumn("product_category_name",
                    F.element_at(
                        F.array([F.lit(c) for c in PRODUCT_CATEGORIES]),
                        (F.rand() * len(PRODUCT_CATEGORIES)).cast(IntegerType()) + 1
                    ))
        .withColumn("product_name_lenght", (F.rand() * 60 + 5).cast(IntegerType()))
        .withColumn("product_description_lenght", (F.rand() * 3000 + 50).cast(IntegerType()))
        .withColumn("product_photos_qty", (F.rand() * 6 + 1).cast(IntegerType()))
        .withColumn("product_weight_g", (F.rand() * 30000 + 100).cast(IntegerType()))
        .withColumn("product_length_cm", (F.rand() * 80 + 5).cast(IntegerType()))
        .withColumn("product_height_cm", (F.rand() * 60 + 2).cast(IntegerType()))
        .withColumn("product_width_cm", (F.rand() * 60 + 5).cast(IntegerType()))
        .drop("row_id")
    )

    output = f"{OUTPUT_PATH}/olist_products_dataset.csv"
    df.coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite").csv(output)
    log_progress("products", count, scale)


def generate_geolocation(spark: SparkSession, scale: int) -> None:
    """Generate synthetic geolocation records."""
    count = get_scaled_count("geolocation", scale)
    print(f"\n🏭 Generating: geolocation ({count:,} records)")

    df = spark.range(0, count).toDF("row_id")

    # Brazil bounding box: lat -33.75 to 5.27, lng -73.99 to -34.79
    df = (
        df
        .withColumn("geolocation_zip_code_prefix",
                    F.lpad((F.rand() * 99000 + 1000).cast(IntegerType()).cast(StringType()), 5, "0"))
        .withColumn("geolocation_lat", F.rand() * -38.0 + 5.0)    # -33 to 5
        .withColumn("geolocation_lng", F.rand() * -39.0 + (-34.0)) # -73 to -34
        .withColumn("geolocation_city",
                    F.concat(F.lit("city_"), (F.rand() * 5000).cast(IntegerType()).cast(StringType())))
        .withColumn("geolocation_state",
                    F.element_at(
                        F.array([F.lit(s) for s, _ in BR_STATES]),
                        (F.rand() * len(BR_STATES)).cast(IntegerType()) + 1
                    ))
        .drop("row_id")
    )

    output = f"{OUTPUT_PATH}/olist_geolocation_dataset.csv"
    df.coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite").csv(output)
    log_progress("geolocation", count, scale)


def generate_orders(spark: SparkSession, scale: int) -> None:
    """
    Generate synthetic orders referencing customer_ids.
    Must run AFTER customers are generated.
    """
    count = get_scaled_count("orders", scale)
    print(f"\n🏭 Generating: orders ({count:,} records)")

    # Referential integrity is guaranteed by deterministic IDs:
    # order.customer_id = "cust_<n>" for n in [0, num_customers).
    num_customers = get_scaled_count("customers", scale)

    # Generate orders
    df = spark.range(0, count).toDF("row_id")

    # Date range: 2016-09-01 to 2018-10-17 (same as original)
    start_ts = int(datetime(2016, 9, 1).timestamp())
    end_ts = int(datetime(2018, 10, 17).timestamp())
    date_range = end_ts - start_ts

    df = (
        df
        .withColumn("order_id", _make_id("order_", F.col("row_id")))
        # Reference an existing customer deterministically (cycles through them)
        .withColumn("customer_id", _make_id("cust_", F.col("row_id") % num_customers))
        # Status: weighted random
        .withColumn("order_status",
                    F.element_at(
                        F.array([F.lit(s) for s in ORDER_STATUSES]),
                        (F.rand() * len(ORDER_STATUSES)).cast(IntegerType()) + 1
                    ))
        # Timestamps
        .withColumn("order_purchase_timestamp",
                    F.from_unixtime(F.lit(start_ts) + (F.rand() * date_range).cast(LongType())))
        .withColumn("order_approved_at",
                    F.from_unixtime(
                        F.unix_timestamp("order_purchase_timestamp") +
                        (F.rand() * 86400 * 2).cast(LongType())  # 0-2 days after purchase
                    ))
        .withColumn("order_delivered_carrier_date",
                    F.from_unixtime(
                        F.unix_timestamp("order_approved_at") +
                        (F.rand() * 86400 * 5).cast(LongType())  # 0-5 days after approval
                    ))
        .withColumn("order_delivered_customer_date",
                    F.from_unixtime(
                        F.unix_timestamp("order_delivered_carrier_date") +
                        (F.rand() * 86400 * 20).cast(LongType())  # 0-20 days after carrier
                    ))
        .withColumn("order_estimated_delivery_date",
                    F.from_unixtime(
                        F.unix_timestamp("order_purchase_timestamp") +
                        (F.rand() * 86400 * 30 + 86400 * 7).cast(LongType())  # 7-37 days
                    ))
    )

    # Join with customers to get actual customer_ids (referential integrity)
    df = df.drop("row_id")

    output = f"{OUTPUT_PATH}/olist_orders_dataset.csv"
    df.select("order_id", "customer_id", "order_status",
              "order_purchase_timestamp", "order_approved_at",
              "order_delivered_carrier_date", "order_delivered_customer_date",
              "order_estimated_delivery_date") \
      .coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite").csv(output)
    log_progress("orders", count, scale)


def generate_order_items(spark: SparkSession, scale: int) -> None:
    """
    Generate order items referencing orders, products, and sellers.
    ~1.13 items per order on average.
    """
    count = get_scaled_count("order_items", scale)
    print(f"\n🏭 Generating: order_items ({count:,} records)")

    # Counts come straight from the scale factor — no read-back or join needed.
    num_orders = get_scaled_count("orders", scale)
    num_products = get_scaled_count("products", scale)
    num_sellers = get_scaled_count("sellers", scale)

    df = spark.range(0, count).toDF("row_id")

    df = (
        df
        # Reference existing parents deterministically (guaranteed to exist)
        .withColumn("order_id", _make_id("order_", F.col("row_id") % num_orders))
        .withColumn("product_id", _make_id("prod_", (F.rand() * num_products).cast(LongType())))
        .withColumn("seller_id", _make_id("sell_", (F.rand() * num_sellers).cast(LongType())))
        .withColumn("order_item_id", (F.col("row_id") % 5 + 1).cast(IntegerType()))
        .withColumn("shipping_limit_date", F.lit("2018-06-01 00:00:00"))
        .withColumn("price", F.round(F.rand() * 500 + 10, 2))
        .withColumn("freight_value", F.round(F.rand() * 80 + 5, 2))
        .drop("row_id")
    )

    output = f"{OUTPUT_PATH}/olist_order_items_dataset.csv"
    df.select("order_id", "order_item_id", "product_id", "seller_id",
              "shipping_limit_date", "price", "freight_value") \
      .coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite").csv(output)
    log_progress("order_items", count, scale)


def generate_order_payments(spark: SparkSession, scale: int) -> None:
    """Generate payment records referencing orders."""
    count = get_scaled_count("order_payments", scale)
    print(f"\n🏭 Generating: order_payments ({count:,} records)")

    num_orders = get_scaled_count("orders", scale)
    df = spark.range(0, count).toDF("row_id")

    df = (
        df
        .withColumn("order_id", _make_id("order_", F.col("row_id") % num_orders))
        .withColumn("payment_sequential", (F.col("row_id") % 3 + 1).cast(IntegerType()))
        .withColumn("payment_type",
                    F.element_at(
                        F.array([F.lit(p) for p in PAYMENT_TYPES]),
                        (F.rand() * len(PAYMENT_TYPES)).cast(IntegerType()) + 1
                    ))
        .withColumn("payment_installments", (F.rand() * 12 + 1).cast(IntegerType()))
        .withColumn("payment_value", F.round(F.rand() * 600 + 15, 2))
        .drop("row_id")
    )

    output = f"{OUTPUT_PATH}/olist_order_payments_dataset.csv"
    df.select("order_id", "payment_sequential", "payment_type",
              "payment_installments", "payment_value") \
      .coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite").csv(output)
    log_progress("order_payments", count, scale)


def generate_order_reviews(spark: SparkSession, scale: int) -> None:
    """Generate review records referencing orders."""
    count = get_scaled_count("order_reviews", scale)
    print(f"\n🏭 Generating: order_reviews ({count:,} records)")

    num_orders = get_scaled_count("orders", scale)
    df = spark.range(0, count).toDF("row_id")

    df = (
        df
        .withColumn("review_id", F.expr("uuid()"))
        .withColumn("order_id", _make_id("order_", F.col("row_id") % num_orders))
        # Score distribution: skewed toward 5 (like real reviews)
        .withColumn("review_score",
                    F.when(F.rand() < 0.55, 5)
                     .when(F.rand() < 0.70, 4)
                     .when(F.rand() < 0.82, 3)
                     .when(F.rand() < 0.90, 2)
                     .otherwise(1))
        .withColumn("review_comment_title",
                    F.when(F.rand() < 0.3, F.lit(None)).otherwise(F.lit("review title")))
        .withColumn("review_comment_message",
                    F.when(F.rand() < 0.4, F.lit(None)).otherwise(F.lit("review comment message")))
        .withColumn("review_creation_date",
                    F.from_unixtime(F.lit(1483228800) + (F.rand() * 63072000).cast(LongType())))
        .withColumn("review_answer_timestamp",
                    F.from_unixtime(
                        F.unix_timestamp("review_creation_date") +
                        (F.rand() * 86400 * 3).cast(LongType())
                    ))
    )

    df = df.drop("row_id")

    output = f"{OUTPUT_PATH}/olist_order_reviews_dataset.csv"
    df.select("review_id", "order_id", "review_score", "review_comment_title",
              "review_comment_message", "review_creation_date", "review_answer_timestamp") \
      .coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite").csv(output)
    log_progress("order_reviews", count, scale)


# ============================================================
# MAIN
# ============================================================
def generate_category_translation(spark: SparkSession, scale: int) -> None:
    """
    Generate the product category translation table.

    This table is static (independent of scale). It MUST contain every
    product_category_name emitted by generate_products() so the Gold
    dim_product join resolves an English category name for each product.
    Previously this table was never generated, which broke the synthetic
    Bronze ingestion (the CSV did not exist) and left dim_product without
    English categories.
    """
    print(f"\n🏭 Generating: category_translation ({len(PRODUCT_CATEGORIES)} records)")

    rows = [(c, c.replace("_", " ")) for c in PRODUCT_CATEGORIES]
    df = spark.createDataFrame(
        rows, ["product_category_name", "product_category_name_english"]
    )

    output = f"{OUTPUT_PATH}/product_category_name_translation.csv"
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output)
    log_progress("category_translation", len(PRODUCT_CATEGORIES), scale)


def generate_all(scale: int = 1000):
    """
    Generate all synthetic tables at the given scale factor.

    Order matters! Base entities first, then transactional tables
    that reference them (referential integrity).
    """

    print("=" * 60)
    print(f"SYNTHETIC DATA GENERATION — {scale}x SCALE")
    print("=" * 60)

    # Estimate total size
    total_records = sum(
        get_scaled_count(t, scale) for t in BASE_COUNTS
    )
    est_size_gb = total_records * 0.0001 / 1024  # rough: 100 bytes/row
    print(f"\n   Target: {total_records:,.0f} total records (~{est_size_gb:.1f} GB)")
    print(f"   Output: {OUTPUT_PATH}")
    print(f"   Scale:  {scale}x original dataset")

    spark = get_spark_session(app_name=f"DataGen_{scale}x")

    # Increase shuffle partitions for large data
    if scale >= 100:
        spark.conf.set("spark.sql.shuffle.partitions", "200")
    if scale >= 1000:
        spark.conf.set("spark.sql.shuffle.partitions", "500")

    start_time = datetime.now()

    # --- Phase 1: Base entities (no foreign keys) ---
    print("\n" + "-" * 40)
    print("PHASE 1: Base Entities")
    print("-" * 40)
    generate_customers(spark, scale)
    generate_sellers(spark, scale)
    generate_products(spark, scale)
    generate_geolocation(spark, scale)
    generate_category_translation(spark, scale)

    # --- Phase 2: Transactional (references base entities) ---
    print("\n" + "-" * 40)
    print("PHASE 2: Transactional Tables")
    print("-" * 40)
    generate_orders(spark, scale)
    generate_order_items(spark, scale)
    generate_order_payments(spark, scale)
    generate_order_reviews(spark, scale)

    # --- Summary ---
    elapsed = (datetime.now() - start_time).total_seconds()
    print("\n" + "=" * 60)
    print("GENERATION COMPLETE")
    print("=" * 60)
    print(f"   Total records: {total_records:,.0f}")
    print(f"   Estimated size: ~{est_size_gb:.1f} GB")
    print(f"   Time elapsed: {elapsed:.0f} seconds ({elapsed/60:.1f} minutes)")
    print(f"   Output path: {OUTPUT_PATH}")
    print("=" * 60)


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic Olist data at scale")
    parser.add_argument("--scale", type=int, default=1000,
                        help="Scale factor (default: 1000 = ~1.55B records)")
    args = parser.parse_args()

    if args.scale < 1:
        print("Scale must be >= 1")
        sys.exit(1)

    generate_all(scale=args.scale)
