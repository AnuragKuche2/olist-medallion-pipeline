# tests/test_bronze.py
"""
Unit tests for Bronze layer ingestion logic.

These tests import and call the REAL functions from src/bronze/ingest.py:
  - split_corrupt_records(): separates good from malformed rows
  - add_ingestion_metadata(): adds audit columns
  - Schema definitions from src/utils/schema_definitions.py

This ensures changes to the real codebase are caught by tests.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime

# Import real functions and configs under test
from src.bronze.ingest import (
    split_corrupt_records,
    add_ingestion_metadata,
    TABLE_CONFIGS,
)
from src.utils.schema_definitions import ORDERS_SCHEMA, CUSTOMERS_SCHEMA


# ============================================================
# CORRUPT RECORD SPLITTING
# ============================================================
class TestSplitCorruptRecords:
    """Tests the real split_corrupt_records() function."""

    def test_good_and_bad_separated(self, spark):
        """Good rows (null _corrupt_record) vs bad rows should be split."""
        data = [
            ("1", "Alice", "100", None),
            ("2", "Bob", "200", None),
            (None, None, None, "malformed,row,with,extra,cols"),
        ]
        df = spark.createDataFrame(data, ["id", "name", "value", "_corrupt_record"])

        good_df, bad_df = split_corrupt_records(df)

        assert good_df.count() == 2
        assert bad_df.count() == 1

    def test_good_df_has_no_corrupt_column(self, spark):
        """Good DataFrame should not have _corrupt_record column."""
        data = [
            ("1", "Alice", "100", None),
        ]
        df = spark.createDataFrame(data, ["id", "name", "value", "_corrupt_record"])

        good_df, _ = split_corrupt_records(df)
        assert "_corrupt_record" not in good_df.columns

    def test_bad_df_retains_corrupt_column(self, spark):
        """Bad DataFrame should retain the _corrupt_record content."""
        data = [
            (None, None, None, "this,is,corrupt"),
        ]
        df = spark.createDataFrame(data, ["id", "name", "value", "_corrupt_record"])

        _, bad_df = split_corrupt_records(df)
        assert "_corrupt_record" in bad_df.columns
        assert bad_df.collect()[0]._corrupt_record == "this,is,corrupt"

    def test_all_good_records(self, spark):
        """When all records are valid, bad_df should be empty."""
        data = [
            ("1", "Alice", "100", None),
            ("2", "Bob", "200", None),
        ]
        df = spark.createDataFrame(data, ["id", "name", "value", "_corrupt_record"])

        good_df, bad_df = split_corrupt_records(df)
        assert good_df.count() == 2
        assert bad_df.count() == 0

    def test_all_bad_records(self, spark):
        """When all records are corrupt, good_df should be empty."""
        data = [
            (None, None, None, "bad1"),
            (None, None, None, "bad2"),
        ]
        df = spark.createDataFrame(data, ["id", "name", "value", "_corrupt_record"])

        good_df, bad_df = split_corrupt_records(df)
        assert good_df.count() == 0
        assert bad_df.count() == 2


# ============================================================
# INGESTION METADATA
# ============================================================
class TestAddIngestionMetadata:
    """Tests the real add_ingestion_metadata() function."""

    def test_metadata_columns_added(self, spark):
        """Should add _ingestion_timestamp, _source_file, _batch_id."""
        data = [("1", "Alice"), ("2", "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])

        enriched = add_ingestion_metadata(
            df,
            source_file="orders.csv",
            batch_id="batch-123",
            ingestion_time="2024-01-15T10:30:00",
        )

        assert "_ingestion_timestamp" in enriched.columns
        assert "_source_file" in enriched.columns
        assert "_batch_id" in enriched.columns
        assert enriched.count() == 2

    def test_metadata_values_correct(self, spark):
        """Metadata column values should match what was passed."""
        data = [("1", "Alice")]
        df = spark.createDataFrame(data, ["id", "name"])

        enriched = add_ingestion_metadata(
            df,
            source_file="customers.csv",
            batch_id="batch-xyz",
            ingestion_time="2024-06-01T08:00:00",
        )

        row = enriched.collect()[0]
        assert row._source_file == "customers.csv"
        assert row._batch_id == "batch-xyz"
        assert row._ingestion_timestamp == "2024-06-01T08:00:00"

    def test_batch_id_consistent_across_rows(self, spark):
        """All rows in a batch should share the same batch_id."""
        data = [("1",), ("2",), ("3",)]
        df = spark.createDataFrame(data, ["id"])

        enriched = add_ingestion_metadata(df, "file.csv", "batch-001", "2024-01-01T00:00:00")

        distinct_batches = enriched.select("_batch_id").distinct().count()
        assert distinct_batches == 1


# ============================================================
# SCHEMA DEFINITIONS
# ============================================================
class TestSchemaDefinitions:
    """Tests that schema configs are complete and correct."""

    def test_all_nine_tables_configured(self):
        """TABLE_CONFIGS should have all 9 Olist tables."""
        expected = {
            "orders", "order_items", "order_payments", "order_reviews",
            "customers", "products", "sellers", "geolocation", "category_translation",
        }
        assert set(TABLE_CONFIGS.keys()) == expected

    def test_each_config_has_required_keys(self):
        """Each table config must have source_file, schema, bronze_table."""
        for table, config in TABLE_CONFIGS.items():
            assert "source_file" in config, f"{table} missing source_file"
            assert "schema" in config, f"{table} missing schema"
            assert "bronze_table" in config, f"{table} missing bronze_table"

    def test_orders_schema_has_order_id(self):
        """ORDERS_SCHEMA should have order_id as first field."""
        assert ORDERS_SCHEMA.fieldNames()[0] == "order_id"

    def test_customers_schema_has_customer_id(self):
        """CUSTOMERS_SCHEMA should have customer_id as first field."""
        assert CUSTOMERS_SCHEMA.fieldNames()[0] == "customer_id"

    def test_schemas_include_corrupt_record_field(self):
        """Each schema should include _corrupt_record for PERMISSIVE mode."""
        for table, config in TABLE_CONFIGS.items():
            schema = config["schema"]
            field_names = schema.fieldNames()
            assert "_corrupt_record" in field_names, (
                f"{table} schema missing _corrupt_record field"
            )
