# src/datagen/generate_standalone.py
"""
Standalone 1000x Data Generator for EMR Serverless.

Self-contained — no local imports. Just upload to S3 and submit.

Usage (EMR Serverless):
  aws emr-serverless start-job-run \
    --application-id <app-id> \
    --execution-role-arn <role-arn> \
    --job-driver '{
      "sparkSubmit": {
        "entryPoint": "s3://anukuche-olist-datalake/code/generate_standalone.py",
        "entryPointArguments": ["--scale", "1000"],
        "sparkSubmitParameters": "--conf spark.sql.shuffle.partitions=500 --conf spark.driver.memory=4g --conf spark.executor.memory=12g"
      }
    }'
"""

import argparse
import sys
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, LongType


# ============================================================
# CONFIG
# ============================================================
S3_BUCKET = "anukuche-olist-datalake"
OUTPUT_PATH = f"s3a://{S3_BUCKET}/landing_synthetic_1000x"

BASE_COUNTS = {
    "customers": 99_441,
    "sellers": 3_095,
    "products": 32_951,
    "geolocation": 19_015,
    "orders": 99_441,
    "order_items": 112_650,
    "order_payments": 103_886,
    "order_reviews": 99_224,
}

BR_STATES = ["SP","RJ","MG","BA","RS","PR","PE","CE","PA","MA","SC","GO",
             "PB","AM","ES","RN","AL","PI","MT","DF","MS","SE","RO","TO","AC","AP","RR"]

ORDER_STATUSES = ["delivered","shipped","canceled","unavailable","processing"]
PAYMENT_TYPES = ["credit_card","boleto","voucher","debit_card"]

PRODUCT_CATEGORIES = [
    "bed_bath_table","health_beauty","sports_leisure","furniture_decor",
    "computers_accessories","housewares","watches_gifts","telephony",
    "garden_tools","auto","toys","cool_stuff","perfumery","babies",
    "electronics","stationery","fashion_bags_accessories","computers",
    "home_comfort","luggage_accessories","consoles_games","food_drink",
    "music","construction_tools_safety","pet_shop","small_appliances",
]


# ============================================================
# SPARK SESSION
# ============================================================
def get_spark():
    return (
        SparkSession.builder
        .appName("DataGen_1000x")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ============================================================
# GENERATORS
# ============================================================
def generate_customers(spark, scale):
    count = BASE_COUNTS["customers"] * scale
    print(f"\n🏭 Generating: customers ({count:,})")

    df = spark.range(0, count).toDF("row_id")
    df = (
        df
        .withColumn("customer_id", F.expr("uuid()"))
        .withColumn("customer_unique_id", F.expr("uuid()"))
        .withColumn("customer_zip_code_prefix",
                    F.lpad((F.rand() * 99000 + 1000).cast(IntegerType()).cast("string"), 5, "0"))
        .withColumn("customer_city",
                    F.concat(F.lit("city_"), (F.rand() * 5000).cast(IntegerType()).cast("string")))
        .withColumn("customer_state",
                    F.element_at(F.array([F.lit(s) for s in BR_STATES]),
                                 (F.rand() * len(BR_STATES)).cast(IntegerType()) + 1))
        .drop("row_id")
    )

    df.coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite") \
      .csv(f"{OUTPUT_PATH}/olist_customers_dataset.csv")
    print(f"   ✅ Done: {count:,}")


def generate_sellers(spark, scale):
    count = BASE_COUNTS["sellers"] * scale
    print(f"\n🏭 Generating: sellers ({count:,})")

    df = spark.range(0, count).toDF("row_id")
    df = (
        df
        .withColumn("seller_id", F.expr("uuid()"))
        .withColumn("seller_zip_code_prefix",
                    F.lpad((F.rand() * 99000 + 1000).cast(IntegerType()).cast("string"), 5, "0"))
        .withColumn("seller_city",
                    F.concat(F.lit("city_"), (F.rand() * 2000).cast(IntegerType()).cast("string")))
        .withColumn("seller_state",
                    F.element_at(F.array([F.lit(s) for s in BR_STATES]),
                                 (F.rand() * len(BR_STATES)).cast(IntegerType()) + 1))
        .drop("row_id")
    )

    df.coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite") \
      .csv(f"{OUTPUT_PATH}/olist_sellers_dataset.csv")
    print(f"   ✅ Done: {count:,}")


def generate_products(spark, scale):
    count = BASE_COUNTS["products"] * scale
    print(f"\n🏭 Generating: products ({count:,})")

    df = spark.range(0, count).toDF("row_id")
    df = (
        df
        .withColumn("product_id", F.expr("uuid()"))
        .withColumn("product_category_name",
                    F.element_at(F.array([F.lit(c) for c in PRODUCT_CATEGORIES]),
                                 (F.rand() * len(PRODUCT_CATEGORIES)).cast(IntegerType()) + 1))
        .withColumn("product_name_lenght", (F.rand() * 60 + 5).cast(IntegerType()))
        .withColumn("product_description_lenght", (F.rand() * 3000 + 50).cast(IntegerType()))
        .withColumn("product_photos_qty", (F.rand() * 6 + 1).cast(IntegerType()))
        .withColumn("product_weight_g", (F.rand() * 30000 + 100).cast(IntegerType()))
        .withColumn("product_length_cm", (F.rand() * 80 + 5).cast(IntegerType()))
        .withColumn("product_height_cm", (F.rand() * 60 + 2).cast(IntegerType()))
        .withColumn("product_width_cm", (F.rand() * 60 + 5).cast(IntegerType()))
        .drop("row_id")
    )

    df.coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite") \
      .csv(f"{OUTPUT_PATH}/olist_products_dataset.csv")
    print(f"   ✅ Done: {count:,}")


def generate_geolocation(spark, scale):
    count = BASE_COUNTS["geolocation"] * scale
    print(f"\n🏭 Generating: geolocation ({count:,})")

    df = spark.range(0, count).toDF("row_id")
    df = (
        df
        .withColumn("geolocation_zip_code_prefix",
                    F.lpad((F.rand() * 99000 + 1000).cast(IntegerType()).cast("string"), 5, "0"))
        .withColumn("geolocation_lat", F.rand() * -38.0 + 5.0)
        .withColumn("geolocation_lng", F.rand() * -39.0 + (-34.0))
        .withColumn("geolocation_city",
                    F.concat(F.lit("city_"), (F.rand() * 5000).cast(IntegerType()).cast("string")))
        .withColumn("geolocation_state",
                    F.element_at(F.array([F.lit(s) for s in BR_STATES]),
                                 (F.rand() * len(BR_STATES)).cast(IntegerType()) + 1))
        .drop("row_id")
    )

    df.coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite") \
      .csv(f"{OUTPUT_PATH}/olist_geolocation_dataset.csv")
    print(f"   ✅ Done: {count:,}")


def generate_orders(spark, scale):
    count = BASE_COUNTS["orders"] * scale
    print(f"\n🏭 Generating: orders ({count:,})")

    customers = spark.read.option("header", "true") \
        .csv(f"{OUTPUT_PATH}/olist_customers_dataset.csv").select("customer_id")
    num_customers = customers.count()

    start_ts = int(datetime(2016, 9, 1).timestamp())
    date_range = int(datetime(2018, 10, 17).timestamp()) - start_ts

    df = spark.range(0, count).toDF("row_id")
    df = (
        df
        .withColumn("order_id", F.expr("uuid()"))
        .withColumn("customer_idx", (F.col("row_id") % num_customers).cast(LongType()))
        .withColumn("order_status",
                    F.element_at(F.array([F.lit(s) for s in ORDER_STATUSES]),
                                 (F.rand() * len(ORDER_STATUSES)).cast(IntegerType()) + 1))
        .withColumn("order_purchase_timestamp",
                    F.from_unixtime(F.lit(start_ts) + (F.rand() * date_range).cast(LongType())))
        .withColumn("order_approved_at",
                    F.from_unixtime(F.unix_timestamp("order_purchase_timestamp") + (F.rand() * 172800).cast(LongType())))
        .withColumn("order_delivered_carrier_date",
                    F.from_unixtime(F.unix_timestamp("order_approved_at") + (F.rand() * 432000).cast(LongType())))
        .withColumn("order_delivered_customer_date",
                    F.from_unixtime(F.unix_timestamp("order_delivered_carrier_date") + (F.rand() * 1728000).cast(LongType())))
        .withColumn("order_estimated_delivery_date",
                    F.from_unixtime(F.unix_timestamp("order_purchase_timestamp") + (F.rand() * 2592000 + 604800).cast(LongType())))
    )

    customers_idx = customers.withColumn("customer_idx", F.monotonically_increasing_id())
    df = df.join(F.broadcast(customers_idx), "customer_idx", "left").drop("customer_idx", "row_id")

    df.select("order_id", "customer_id", "order_status", "order_purchase_timestamp",
              "order_approved_at", "order_delivered_carrier_date",
              "order_delivered_customer_date", "order_estimated_delivery_date") \
      .coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite") \
      .csv(f"{OUTPUT_PATH}/olist_orders_dataset.csv")
    print(f"   ✅ Done: {count:,}")


def generate_order_items(spark, scale):
    count = BASE_COUNTS["order_items"] * scale
    print(f"\n🏭 Generating: order_items ({count:,})")

    orders = spark.read.option("header", "true").csv(f"{OUTPUT_PATH}/olist_orders_dataset.csv").select("order_id")
    products = spark.read.option("header", "true").csv(f"{OUTPUT_PATH}/olist_products_dataset.csv").select("product_id")
    sellers = spark.read.option("header", "true").csv(f"{OUTPUT_PATH}/olist_sellers_dataset.csv").select("seller_id")

    num_orders = orders.count()
    num_products = products.count()
    num_sellers = sellers.count()

    df = spark.range(0, count).toDF("row_id")
    df = (
        df
        .withColumn("order_idx", (F.col("row_id") % num_orders).cast(LongType()))
        .withColumn("product_idx", (F.rand() * num_products).cast(LongType()))
        .withColumn("seller_idx", (F.rand() * num_sellers).cast(LongType()))
        .withColumn("order_item_id", (F.col("row_id") % 5 + 1).cast(IntegerType()))
        .withColumn("shipping_limit_date", F.lit("2018-06-01 00:00:00"))
        .withColumn("price", F.round(F.rand() * 500 + 10, 2))
        .withColumn("freight_value", F.round(F.rand() * 80 + 5, 2))
    )

    orders_idx = orders.withColumn("order_idx", F.monotonically_increasing_id())
    products_idx = products.withColumn("product_idx", F.monotonically_increasing_id())
    sellers_idx = sellers.withColumn("seller_idx", F.monotonically_increasing_id())

    df = (
        df
        .join(F.broadcast(orders_idx), "order_idx", "left")
        .join(F.broadcast(products_idx), "product_idx", "left")
        .join(F.broadcast(sellers_idx), "seller_idx", "left")
        .drop("order_idx", "product_idx", "seller_idx", "row_id")
    )

    df.select("order_id", "order_item_id", "product_id", "seller_id",
              "shipping_limit_date", "price", "freight_value") \
      .coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite") \
      .csv(f"{OUTPUT_PATH}/olist_order_items_dataset.csv")
    print(f"   ✅ Done: {count:,}")


def generate_order_payments(spark, scale):
    count = BASE_COUNTS["order_payments"] * scale
    print(f"\n🏭 Generating: order_payments ({count:,})")

    orders = spark.read.option("header", "true").csv(f"{OUTPUT_PATH}/olist_orders_dataset.csv").select("order_id")
    num_orders = orders.count()

    df = spark.range(0, count).toDF("row_id")
    df = (
        df
        .withColumn("order_idx", (F.col("row_id") % num_orders).cast(LongType()))
        .withColumn("payment_sequential", (F.col("row_id") % 3 + 1).cast(IntegerType()))
        .withColumn("payment_type",
                    F.element_at(F.array([F.lit(p) for p in PAYMENT_TYPES]),
                                 (F.rand() * len(PAYMENT_TYPES)).cast(IntegerType()) + 1))
        .withColumn("payment_installments", (F.rand() * 12 + 1).cast(IntegerType()))
        .withColumn("payment_value", F.round(F.rand() * 600 + 15, 2))
    )

    orders_idx = orders.withColumn("order_idx", F.monotonically_increasing_id())
    df = df.join(F.broadcast(orders_idx), "order_idx", "left").drop("order_idx", "row_id")

    df.select("order_id", "payment_sequential", "payment_type",
              "payment_installments", "payment_value") \
      .coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite") \
      .csv(f"{OUTPUT_PATH}/olist_order_payments_dataset.csv")
    print(f"   ✅ Done: {count:,}")


def generate_order_reviews(spark, scale):
    count = BASE_COUNTS["order_reviews"] * scale
    print(f"\n🏭 Generating: order_reviews ({count:,})")

    orders = spark.read.option("header", "true").csv(f"{OUTPUT_PATH}/olist_orders_dataset.csv").select("order_id")
    num_orders = orders.count()

    df = spark.range(0, count).toDF("row_id")
    df = (
        df
        .withColumn("review_id", F.expr("uuid()"))
        .withColumn("order_idx", (F.col("row_id") % num_orders).cast(LongType()))
        .withColumn("review_score",
                    F.when(F.rand() < 0.55, 5)
                     .when(F.rand() < 0.70, 4)
                     .when(F.rand() < 0.82, 3)
                     .when(F.rand() < 0.90, 2)
                     .otherwise(1))
        .withColumn("review_comment_title",
                    F.when(F.rand() < 0.3, F.lit(None)).otherwise(F.lit("review title")))
        .withColumn("review_comment_message",
                    F.when(F.rand() < 0.4, F.lit(None)).otherwise(F.lit("review comment")))
        .withColumn("review_creation_date",
                    F.from_unixtime(F.lit(1483228800) + (F.rand() * 63072000).cast(LongType())))
        .withColumn("review_answer_timestamp",
                    F.from_unixtime(F.unix_timestamp("review_creation_date") + (F.rand() * 259200).cast(LongType())))
    )

    orders_idx = orders.withColumn("order_idx", F.monotonically_increasing_id())
    df = df.join(F.broadcast(orders_idx), "order_idx", "left").drop("order_idx", "row_id")

    df.select("review_id", "order_id", "review_score", "review_comment_title",
              "review_comment_message", "review_creation_date", "review_answer_timestamp") \
      .coalesce(max(1, count // 5_000_000)) \
      .write.option("header", "true").mode("overwrite") \
      .csv(f"{OUTPUT_PATH}/olist_order_reviews_dataset.csv")
    print(f"   ✅ Done: {count:,}")


# ============================================================
# MAIN
# ============================================================
def main(scale):
    print("=" * 60)
    print(f"SYNTHETIC DATA GENERATION — {scale}x SCALE (EMR Serverless)")
    print("=" * 60)

    total = sum(v * scale for v in BASE_COUNTS.values())
    print(f"\n   Target: {total:,} records (~{total * 0.0001 / 1024:.1f} GB)")
    print(f"   Output: {OUTPUT_PATH}")

    spark = get_spark()
    start = datetime.now()

    # Phase 1: Base entities
    generate_customers(spark, scale)
    generate_sellers(spark, scale)
    generate_products(spark, scale)
    generate_geolocation(spark, scale)

    # Phase 2: Transactional (references base)
    generate_orders(spark, scale)
    generate_order_items(spark, scale)
    generate_order_payments(spark, scale)
    generate_order_reviews(spark, scale)

    elapsed = (datetime.now() - start).total_seconds()
    print("\n" + "=" * 60)
    print(f"COMPLETE — {total:,} records in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, default=1000)
    args = parser.parse_args()
    main(args.scale)
