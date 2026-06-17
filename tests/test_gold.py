# tests/test_gold.py
"""
Unit tests for Gold layer star schema logic.

Tests:
  - Surrogate key generation
  - Date dimension completeness
  - Seller tier classification
  - Fact table aggregation logic
  - Referential integrity patterns
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


class TestDimDate:
    """Tests date dimension logic."""

    def test_date_spine_covers_range(self, spark):
        """Date dimension should cover the full date range."""
        df = spark.sql("""
            SELECT explode(sequence(
                to_date('2018-01-01'),
                to_date('2018-01-31'),
                interval 1 day
            )) as full_date
        """)

        assert df.count() == 31  # January has 31 days

    def test_date_key_format(self, spark):
        """date_key should be yyyyMMdd integer format."""
        data = [("2018-06-15",)]
        df = spark.createDataFrame(data, ["date_str"])

        df = df.withColumn("full_date", F.to_date("date_str"))
        df = df.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))

        result = df.collect()[0].date_key
        assert result == 20180615

    def test_is_weekend_flag(self, spark):
        """Saturday and Sunday should be flagged as weekend."""
        data = [
            ("2018-06-16",),  # Saturday
            ("2018-06-17",),  # Sunday
            ("2018-06-18",),  # Monday
        ]
        df = spark.createDataFrame(data, ["date_str"])

        df = df.withColumn("full_date", F.to_date("date_str"))
        df = df.withColumn(
            "is_weekend",
            F.when(F.dayofweek("full_date").isin(1, 7), True).otherwise(False)
        )

        results = [row.is_weekend for row in df.collect()]
        assert results == [True, True, False]


class TestDimSeller:
    """Tests seller dimension logic."""

    def test_seller_tier_platinum(self, spark):
        """Sellers with >= 100 orders should be 'platinum'."""
        data = [(150,), (100,), (99,)]
        df = spark.createDataFrame(data, ["total_orders"])

        df = df.withColumn(
            "seller_tier",
            F.when(F.col("total_orders") >= 100, "platinum")
             .when(F.col("total_orders") >= 50, "gold")
             .when(F.col("total_orders") >= 10, "silver")
             .otherwise("bronze")
        )

        results = [row.seller_tier for row in df.collect()]
        assert results == ["platinum", "platinum", "gold"]

    def test_seller_tier_all_levels(self, spark):
        """All tier thresholds should work correctly."""
        data = [(200,), (75,), (25,), (5,)]
        df = spark.createDataFrame(data, ["total_orders"])

        df = df.withColumn(
            "seller_tier",
            F.when(F.col("total_orders") >= 100, "platinum")
             .when(F.col("total_orders") >= 50, "gold")
             .when(F.col("total_orders") >= 10, "silver")
             .otherwise("bronze")
        )

        results = [row.seller_tier for row in df.collect()]
        assert results == ["platinum", "gold", "silver", "bronze"]


class TestFactAggregation:
    """Tests fact table aggregation patterns."""

    def test_payment_aggregation_to_order_level(self, spark):
        """Multiple payments per order should aggregate correctly."""
        data = [
            ("order_1", "credit_card", 100.0),
            ("order_1", "voucher", 50.0),
            ("order_2", "debit_card", 200.0),
        ]
        df = spark.createDataFrame(data, ["order_id", "payment_type", "payment_value"])

        agg = df.groupBy("order_id").agg(
            F.sum("payment_value").alias("total_payment"),
            F.first("payment_type").alias("primary_payment_type"),
        )

        order_1 = agg.filter(F.col("order_id") == "order_1").collect()[0]
        assert order_1.total_payment == 150.0

        order_2 = agg.filter(F.col("order_id") == "order_2").collect()[0]
        assert order_2.total_payment == 200.0

    def test_item_count_per_order(self, spark):
        """Item count should reflect number of line items per order."""
        data = [
            ("order_1", 1, 50.0),
            ("order_1", 2, 30.0),
            ("order_1", 3, 20.0),
            ("order_2", 1, 100.0),
        ]
        df = spark.createDataFrame(data, ["order_id", "item_id", "price"])

        agg = df.groupBy("order_id").agg(
            F.count("*").alias("item_count"),
            F.sum("price").alias("total_value"),
        )

        order_1 = agg.filter(F.col("order_id") == "order_1").collect()[0]
        assert order_1.item_count == 3
        assert order_1.total_value == 100.0


class TestSurrogateKeys:
    """Tests surrogate key generation."""

    def test_monotonically_increasing_id_unique(self, spark):
        """Surrogate keys should be unique."""
        data = [("A",), ("B",), ("C",), ("D",)]
        df = spark.createDataFrame(data, ["name"])

        df = df.withColumn("key", F.monotonically_increasing_id())

        total = df.count()
        distinct_keys = df.select("key").distinct().count()
        assert total == distinct_keys

    def test_surrogate_key_not_null(self, spark):
        """No surrogate key should be null."""
        data = [("A",), ("B",), ("C",)]
        df = spark.createDataFrame(data, ["name"])

        df = df.withColumn("key", F.monotonically_increasing_id())

        null_keys = df.filter(F.col("key").isNull()).count()
        assert null_keys == 0
