# src/bronze/ingest.py
"""
Generic Bronze Ingestion Module.
One function that ingests ANY table — just pass the config.

Why generic?
  - DRY: Don't repeat the same logic 9 times
  - Maintainable: Fix a bug once, fixed everywhere
  - Extensible: Add a new table? Just add config, no new code
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType
import uuid
import os
from datetime import datetime

from src.utils.spark_session import get_spark_session


# ============================================================
# CONFIGURATION — All 9 tables defined here
# ============================================================
S3_BUCKET = "anukuche-olist-datalake"
# Support synthetic data: set LANDING_FOLDER=landing_synthetic to use generated data
LANDING_FOLDER = os.environ.get("LANDING_FOLDER", "landing")
BRONZE_FOLDER = os.environ.get("BRONZE_FOLDER", "bronze")

# Import all schemas
from src.utils.schema_definitions import (
    ORDERS_SCHEMA,
    ORDER_ITEMS_SCHEMA,
    ORDER_PAYMENTS_SCHEMA,
    ORDER_REVIEWS_SCHEMA,
    CUSTOMERS_SCHEMA,
    PRODUCTS_SCHEMA,
    SELLERS_SCHEMA,
    GEOLOCATION_SCHEMA,
    CATEGORY_TRANSLATION_SCHEMA,
)

# Table configs: name → (csv filename, schema, bronze folder name)
TABLE_CONFIGS = {
    "orders": {
        "source_file": "olist_orders_dataset.csv",
        "schema": ORDERS_SCHEMA,
        "bronze_table": "orders",
    },
    "order_items": {
        "source_file": "olist_order_items_dataset.csv",
        "schema": ORDER_ITEMS_SCHEMA,
        "bronze_table": "order_items",
    },
    "order_payments": {
        "source_file": "olist_order_payments_dataset.csv",
        "schema": ORDER_PAYMENTS_SCHEMA,
        "bronze_table": "order_payments",
    },
    "order_reviews": {
        "source_file": "olist_order_reviews_dataset.csv",
        "schema": ORDER_REVIEWS_SCHEMA,
        "bronze_table": "order_reviews",
    },
    "customers": {
        "source_file": "olist_customers_dataset.csv",
        "schema": CUSTOMERS_SCHEMA,
        "bronze_table": "customers",
    },
    "products": {
        "source_file": "olist_products_dataset.csv",
        "schema": PRODUCTS_SCHEMA,
        "bronze_table": "products",
    },
    "sellers": {
        "source_file": "olist_sellers_dataset.csv",
        "schema": SELLERS_SCHEMA,
        "bronze_table": "sellers",
    },
    "geolocation": {
        "source_file": "olist_geolocation_dataset.csv",
        "schema": GEOLOCATION_SCHEMA,
        "bronze_table": "geolocation",
    },
    "category_translation": {
        "source_file": "product_category_name_translation.csv",
        "schema": CATEGORY_TRANSLATION_SCHEMA,
        "bronze_table": "category_translation",
    },
}


# ============================================================
# GENERIC INGESTION FUNCTION
# ============================================================
def ingest_to_bronze(table_name: str) -> int:
    """
    Ingests a single table from landing (CSV) to bronze (Delta).

    Args:
        table_name: Key from TABLE_CONFIGS (e.g., "orders", "customers")

    Returns:
        Number of records written to bronze
    """

    # --- Get config for this table ---
    config = TABLE_CONFIGS[table_name]
    source_file = config["source_file"]
    schema = config["schema"]
    bronze_table = config["bronze_table"]

    source_path = f"s3a://{S3_BUCKET}/{LANDING_FOLDER}/{source_file}"
    bronze_path = f"s3a://{S3_BUCKET}/{BRONZE_FOLDER}/{bronze_table}"
    quarantine_path = f"s3a://{S3_BUCKET}/{BRONZE_FOLDER}_quarantine/{bronze_table}"

    # --- Start Spark ---
    spark = get_spark_session(app_name=f"Bronze_Ingest_{table_name}")
    print(f"\n🚀 Starting bronze ingestion: {table_name}")
    print(f"   Source: {source_path}")
    print(f"   Target: {bronze_path}")

    # --- Read CSV with explicit schema + corrupt record handling ---
    read_schema = schema.add("_corrupt_record", StringType(), True)

    raw_df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(read_schema)
        .csv(source_path)
    )

    # --- Separate good from bad ---
    good_df = raw_df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    bad_df = raw_df.filter(F.col("_corrupt_record").isNotNull())

    # --- Add metadata ---
    batch_id = str(uuid.uuid4())
    ingestion_time = datetime.now().isoformat()

    enriched_df = (
        good_df
        .withColumn("_ingestion_timestamp", F.lit(ingestion_time))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
    )

    # --- Write to Bronze (Delta) ---
    enriched_df.write.format("delta").mode("overwrite").save(bronze_path)

    # --- Quarantine bad records ---
    bad_count = bad_df.count()
    if bad_count > 0:
        bad_df.write.format("delta").mode("append").save(quarantine_path)
        print(f"   ⚠️  Quarantined: {bad_count} bad records")

    # --- Log results ---
    good_count = enriched_df.count()
    print(f"   ✅ Written: {good_count} records")
    print(f"   📋 Batch ID: {batch_id}")

    return good_count


# ============================================================
# INGEST ALL TABLES
# ============================================================
def ingest_all():
    """Ingest all 9 tables from landing to bronze."""

    print("=" * 60)
    print("BRONZE LAYER — Full Ingestion")
    print("=" * 60)

    results = {}
    for table_name in TABLE_CONFIGS:
        count = ingest_to_bronze(table_name)
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
    ingest_all()
