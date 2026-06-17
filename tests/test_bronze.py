# tests/test_bronze.py
"""
Unit tests for Bronze layer ingestion logic.

Tests the core patterns without S3:
  - Schema enforcement
  - Corrupt record detection
  - Metadata column addition
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime


class TestBronzeSchemaEnforcement:
    """Tests that schema enforcement works correctly."""

    def test_valid_records_pass_schema(self, spark, tmp_path_delta):
        """Valid CSV rows should parse correctly with explicit schema."""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
        ])

        # Simulate CSV data as DataFrame
        data = [("1", "Alice", "100"), ("2", "Bob", "200")]
        df = spark.createDataFrame(data, ["id", "name", "value"])

        # Apply schema enforcement (cast)
        enforced = spark.createDataFrame(df.rdd, schema)
        assert enforced.count() == 2
        assert enforced.schema == schema

    def test_corrupt_record_capture(self, spark, tmp_path_delta):
        """Records that don't match schema should be captured in _corrupt_record."""
        # Write a CSV with a bad row
        import os
        csv_path = os.path.join(tmp_path_delta, "test.csv")
        with open(csv_path, "w") as f:
            f.write("id,name,value\n")
            f.write("1,Alice,100\n")
            f.write("2,Bob,200\n")
            f.write("this,has,too,many,columns\n")

        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
        ])

        df = (
            spark.read
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema)
            .csv(csv_path)
        )

        good = df.filter(F.col("_corrupt_record").isNull())
        bad = df.filter(F.col("_corrupt_record").isNotNull())

        assert good.count() == 2
        assert bad.count() == 1


class TestBronzeMetadata:
    """Tests that metadata columns are added correctly."""

    def test_metadata_columns_added(self, spark):
        """Enriched DataFrame should have _ingestion_timestamp, _source_file, _batch_id."""
        data = [("1", "Alice"), ("2", "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])

        # Simulate metadata enrichment
        enriched = (
            df
            .withColumn("_ingestion_timestamp", F.lit(datetime.now().isoformat()))
            .withColumn("_source_file", F.lit("test_file.csv"))
            .withColumn("_batch_id", F.lit("test-batch-123"))
        )

        assert "_ingestion_timestamp" in enriched.columns
        assert "_source_file" in enriched.columns
        assert "_batch_id" in enriched.columns
        assert enriched.count() == 2

    def test_batch_id_is_consistent(self, spark):
        """All rows in a batch should have the same batch_id."""
        data = [("1",), ("2",), ("3",)]
        df = spark.createDataFrame(data, ["id"])

        batch_id = "unique-batch-abc"
        enriched = df.withColumn("_batch_id", F.lit(batch_id))

        distinct_batches = enriched.select("_batch_id").distinct().count()
        assert distinct_batches == 1
