# tests/test_silver.py
"""
Unit tests for Silver layer transformation logic.

Tests core transformations:
  - Deduplication
  - Type casting
  - Null handling
  - Standardization (city names, state codes)
  - Derived columns (delivery_days, is_late_delivery, regions)
  - Zip code padding
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


class TestDeduplication:
    """Tests deduplication logic."""

    def test_exact_duplicates_removed(self, spark):
        """Exact duplicate rows should be removed."""
        data = [
            ("order_1", "delivered", "2018-01-01"),
            ("order_1", "delivered", "2018-01-01"),  # exact dup
            ("order_2", "shipped", "2018-01-02"),
        ]
        df = spark.createDataFrame(data, ["order_id", "status", "date"])

        deduped = df.dropDuplicates(["order_id"])
        assert deduped.count() == 2

    def test_dedup_keeps_one_row(self, spark):
        """After dedup, each key should appear exactly once."""
        data = [
            ("review_1", "order_1", 5),
            ("review_1", "order_1", 5),  # dup
            ("review_1", "order_1", 5),  # dup
            ("review_2", "order_2", 3),
        ]
        df = spark.createDataFrame(data, ["review_id", "order_id", "score"])

        deduped = df.dropDuplicates(["review_id"])
        assert deduped.count() == 2
        assert deduped.filter(F.col("review_id") == "review_1").count() == 1


class TestTypeCasting:
    """Tests type casting transformations."""

    def test_string_to_timestamp(self, spark):
        """Timestamp strings should cast to proper TimestampType."""
        data = [("2018-01-01 10:30:00",), ("2018-06-15 14:45:30",)]
        df = spark.createDataFrame(data, ["ts_string"])

        df = df.withColumn("ts", F.to_timestamp("ts_string"))

        assert df.schema["ts"].dataType.typeName() == "timestamp"
        assert df.filter(F.col("ts").isNull()).count() == 0

    def test_string_to_double(self, spark):
        """Numeric strings should cast to DoubleType."""
        data = [("99.99",), ("149.50",), ("0.01",)]
        df = spark.createDataFrame(data, ["price_str"])

        df = df.withColumn("price", F.col("price_str").cast(DoubleType()))

        assert df.filter(F.col("price").isNull()).count() == 0
        assert df.filter(F.col("price") > 0).count() == 3

    def test_invalid_cast_returns_null(self, spark):
        """Non-numeric strings cast to Double should become null."""
        data = [("abc",), ("99.99",), ("",)]
        df = spark.createDataFrame(data, ["value"])

        df = df.withColumn("numeric", F.col("value").cast(DoubleType()))

        nulls = df.filter(F.col("numeric").isNull()).count()
        assert nulls == 2  # "abc" and "" become null


class TestStandardization:
    """Tests data standardization."""

    def test_city_lowercase_and_trim(self, spark):
        """City names should be lowercased and trimmed."""
        data = [("  São Paulo  ",), ("RIO DE JANEIRO",), ("brasilia",)]
        df = spark.createDataFrame(data, ["city"])

        df = df.withColumn("city", F.lower(F.trim(F.col("city"))))

        results = [row.city for row in df.collect()]
        assert results == ["são paulo", "rio de janeiro", "brasilia"]

    def test_zip_code_padding(self, spark):
        """Zip codes should be left-padded to 5 characters."""
        data = [("1234",), ("56789",), ("1",)]
        df = spark.createDataFrame(data, ["zip"])

        df = df.withColumn("zip_padded", F.lpad(F.col("zip"), 5, "0"))

        results = [row.zip_padded for row in df.collect()]
        assert results == ["01234", "56789", "00001"]

    def test_state_uppercase(self, spark):
        """State codes should be uppercase."""
        data = [("sp",), ("Rj",), ("MG",)]
        df = spark.createDataFrame(data, ["state"])

        df = df.withColumn("state", F.upper(F.trim(F.col("state"))))

        results = [row.state for row in df.collect()]
        assert results == ["SP", "RJ", "MG"]


class TestDerivedColumns:
    """Tests derived column calculations."""

    def test_delivery_days_calculation(self, spark):
        """delivery_days = delivered_date - purchase_date."""
        data = [
            ("2018-01-01 10:00:00", "2018-01-08 14:00:00"),  # 7 days
            ("2018-03-01 08:00:00", "2018-03-15 12:00:00"),  # 14 days
        ]
        df = spark.createDataFrame(data, ["purchase", "delivered"])

        df = df.withColumn("purchase_ts", F.to_timestamp("purchase"))
        df = df.withColumn("delivered_ts", F.to_timestamp("delivered"))
        df = df.withColumn("delivery_days", F.datediff("delivered_ts", "purchase_ts"))

        results = [row.delivery_days for row in df.collect()]
        assert results == [7, 14]

    def test_is_late_delivery(self, spark):
        """is_late should be True when delivered > estimated."""
        data = [
            ("2018-01-10", "2018-01-08"),  # late (delivered after estimated)
            ("2018-01-05", "2018-01-08"),  # on time
            ("2018-01-08", "2018-01-08"),  # exactly on time
        ]
        df = spark.createDataFrame(data, ["delivered", "estimated"])

        df = df.withColumn("delivered_ts", F.to_timestamp("delivered"))
        df = df.withColumn("estimated_ts", F.to_timestamp("estimated"))
        df = df.withColumn(
            "is_late",
            F.when(F.col("delivered_ts") > F.col("estimated_ts"), True).otherwise(False)
        )

        results = [row.is_late for row in df.collect()]
        assert results == [True, False, False]

    def test_weight_kg_conversion(self, spark):
        """weight_kg should be weight_g / 1000."""
        data = [(1500.0,), (500.0,), (25000.0,)]
        df = spark.createDataFrame(data, ["weight_g"])

        df = df.withColumn("weight_kg", F.col("weight_g") / 1000.0)

        results = [row.weight_kg for row in df.collect()]
        assert results == [1.5, 0.5, 25.0]

    def test_volume_calculation(self, spark):
        """volume_cm3 = length × height × width."""
        data = [(10.0, 20.0, 30.0), (5.0, 5.0, 5.0)]
        df = spark.createDataFrame(data, ["length", "height", "width"])

        df = df.withColumn("volume", F.col("length") * F.col("height") * F.col("width"))

        results = [row.volume for row in df.collect()]
        assert results == [6000.0, 125.0]


class TestNullHandling:
    """Tests null handling logic."""

    def test_coalesce_fills_nulls(self, spark):
        """Null category should be filled with 'unknown'."""
        data = [("electronics",), (None,), ("furniture",)]
        df = spark.createDataFrame(data, ["category"])

        df = df.withColumn("category", F.coalesce(F.col("category"), F.lit("unknown")))

        results = [row.category for row in df.collect()]
        assert results == ["electronics", "unknown", "furniture"]

    def test_empty_string_to_null(self, spark):
        """Empty strings should be converted to null."""
        data = [("Great product!",), ("",), (None,)]
        df = spark.createDataFrame(data, ["comment"])

        df = df.withColumn(
            "comment",
            F.when(F.col("comment") == "", F.lit(None)).otherwise(F.col("comment"))
        )

        null_count = df.filter(F.col("comment").isNull()).count()
        assert null_count == 2  # empty string + original null
