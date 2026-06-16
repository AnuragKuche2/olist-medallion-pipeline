# src/bronze/ingest_orders.py
"""
Bronze Layer: Ingest raw orders CSV into Delta Lake.
What this script does:
  1. Reads raw CSV from S3 landing zone
  2. Applies explicit schema (no inferSchema)
  3. Adds metadata columns (when ingested, from where, batch ID)
  4. Writes to Delta format in bronze zone
  5. Bad records go to quarantine (not lost, not crashing the pipeline)

No transformations happen here — that's Silver's job.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import uuid
from datetime import datetime

# Import our reusable utilities
from src.utils.spark_session import get_spark_session
from src.utils.schema_definitions import ORDERS_SCHEMA


# ============================================================
# CONFIGURATION
# ============================================================
S3_BUCKET = "anukuche-olist-datalake"
SOURCE_PATH = f"s3a://{S3_BUCKET}/landing/olist_orders_dataset.csv"
BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/orders"
QUARANTINE_PATH = f"s3a://{S3_BUCKET}/quarantine/orders"


def ingest_orders():
    """
    Main ingestion function for orders table.
    Reads CSV → adds metadata → writes Delta.
    """

    # --- Step 1: Start Spark ---
    spark = get_spark_session(app_name="Bronze_Ingest_Orders")
    print(f"Starting bronze ingestion: orders")
    print(f"Source: {SOURCE_PATH}")
    print(f"Target: {BRONZE_PATH}")

    # --- Step 2: Read CSV with explicit schema ---
    # mode="PERMISSIVE" → doesn't crash on bad rows, puts null instead
    # columnNameOfCorruptRecord → captures entire bad row as string
    raw_df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(ORDERS_SCHEMA.add("_corrupt_record", StringType(), True))
        .csv(SOURCE_PATH)
    )

    # --- Step 3: Separate good records from bad records ---
    good_df = raw_df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    bad_df = raw_df.filter(F.col("_corrupt_record").isNotNull())

    # --- Step 4: Add metadata columns to good records ---
    batch_id = str(uuid.uuid4())  # Unique ID for this run
    ingestion_time = datetime.now().isoformat()

    enriched_df = (
        good_df
        .withColumn("_ingestion_timestamp", F.lit(ingestion_time))
        .withColumn("_source_file", F.lit("olist_orders_dataset.csv"))
        .withColumn("_batch_id", F.lit(batch_id))
    )

    # --- Step 5: Write to Delta (Bronze) ---
    (
        enriched_df.write
        .format("delta")
        .mode("overwrite")  # Full refresh for now; MERGE for incremental later
        .save(BRONZE_PATH)
    )

    # --- Step 6: Write bad records to quarantine ---
    if bad_df.count() > 0:
        (
            bad_df.write
            .format("delta")
            .mode("append")  # Append — don't lose previous quarantined records
            .save(QUARANTINE_PATH)
        )
        print(f"Quarantined {bad_df.count()} bad records")

    # --- Step 7: Log results ---
    records_written = enriched_df.count()
    print(f"Bronze ingestion complete: orders")
    print(f"Records written: {records_written}")
    print(f"Batch ID: {batch_id}")
    print(f"Timestamp: {ingestion_time}")

    return records_written


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    ingest_orders()
