# tests/test_gold.py
"""
Unit tests for Gold layer star schema logic.

These tests import and call the REAL pure helper functions from
src/gold/build.py, ensuring that any regression in surrogate key
generation, date dimension logic, seller tiering, or fact aggregation
is caught by the test suite.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Import real functions under test
from src.gold.build import (
    seller_tier_expr,
    add_date_attributes,
    aggregate_order_payments,
    aggregate_order_items,
)


# ============================================================
# SELLER TIER
# ============================================================
class TestSellerTier:
    """Tests the real seller_tier_expr() function."""

    def test_platinum_threshold(self, spark):
        """Sellers with >= 100 orders should be 'platinum'."""
        data = [(150,), (100,)]
        df = spark.createDataFrame(data, ["total_orders"])

        df = df.withColumn("seller_tier", seller_tier_expr("total_orders"))
        results = [row.seller_tier for row in df.collect()]
        assert results == ["platinum", "platinum"]

    def test_all_tiers(self, spark):
        """All tier thresholds should classify correctly."""
        data = [(200,), (75,), (25,), (5,)]
        df = spark.createDataFrame(data, ["total_orders"])

        df = df.withColumn("seller_tier", seller_tier_expr("total_orders"))
        results = [row.seller_tier for row in df.collect()]
        assert results == ["platinum", "gold", "silver", "bronze"]

    def test_boundary_values(self, spark):
        """Boundary values: 100, 50, 10, 9."""
        data = [(100,), (99,), (50,), (49,), (10,), (9,)]
        df = spark.createDataFrame(data, ["total_orders"])

        df = df.withColumn("seller_tier", seller_tier_expr("total_orders"))
        results = [row.seller_tier for row in df.collect()]
        assert results == ["platinum", "gold", "gold", "silver", "silver", "bronze"]


# ============================================================
# DATE DIMENSION
# ============================================================
class TestAddDateAttributes:
    """Tests the real add_date_attributes() function."""

    def test_date_key_format(self, spark):
        """date_key should be yyyyMMdd integer (e.g. 20180615)."""
        data = [("2018-06-15",)]
        df = spark.createDataFrame(data, ["date_str"])
        df = df.withColumn("full_date", F.to_date("date_str"))

        result = add_date_attributes(df)
        assert result.collect()[0].date_key == 20180615

    def test_year_month_quarter(self, spark):
        """Year, month, quarter should be derived correctly."""
        data = [("2018-09-20",)]
        df = spark.createDataFrame(data, ["date_str"])
        df = df.withColumn("full_date", F.to_date("date_str"))

        result = add_date_attributes(df)
        row = result.collect()[0]
        assert row.year == 2018
        assert row.month == 9
        assert row.quarter == 3

    def test_is_weekend(self, spark):
        """Saturday (7) and Sunday (1) should be flagged as weekend."""
        data = [
            ("2018-06-16",),  # Saturday
            ("2018-06-17",),  # Sunday
            ("2018-06-18",),  # Monday
        ]
        df = spark.createDataFrame(data, ["date_str"])
        df = df.withColumn("full_date", F.to_date("date_str"))

        result = add_date_attributes(df)
        results = [row.is_weekend for row in result.collect()]
        assert results == [True, True, False]

    def test_day_name(self, spark):
        """day_name should return the weekday name."""
        data = [("2018-06-18",)]  # Monday
        df = spark.createDataFrame(data, ["date_str"])
        df = df.withColumn("full_date", F.to_date("date_str"))

        result = add_date_attributes(df)
        assert result.collect()[0].day_name == "Monday"

    def test_year_month_format(self, spark):
        """year_month should be 'yyyy-MM' format."""
        data = [("2018-11-05",)]
        df = spark.createDataFrame(data, ["date_str"])
        df = df.withColumn("full_date", F.to_date("date_str"))

        result = add_date_attributes(df)
        assert result.collect()[0].year_month == "2018-11"


# ============================================================
# FACT AGGREGATIONS
# ============================================================
class TestAggregateOrderPayments:
    """Tests the real aggregate_order_payments() function."""

    def test_multiple_payments_summed(self, spark):
        """Multiple payments per order should sum correctly."""
        data = [
            ("order_1", "credit_card", 100.0, 3),
            ("order_1", "voucher", 50.0, 1),
            ("order_2", "debit_card", 200.0, 1),
        ]
        df = spark.createDataFrame(data, ["order_id", "payment_type", "payment_value", "payment_installments"])

        result = aggregate_order_payments(df).orderBy("order_id")
        rows = result.collect()

        assert rows[0].total_payment_value == 150.0
        assert rows[1].total_payment_value == 200.0

    def test_max_installments(self, spark):
        """max_installments should be the highest across payment rows."""
        data = [
            ("order_1", "credit_card", 100.0, 6),
            ("order_1", "credit_card", 50.0, 3),
        ]
        df = spark.createDataFrame(data, ["order_id", "payment_type", "payment_value", "payment_installments"])

        result = aggregate_order_payments(df)
        assert result.collect()[0].max_installments == 6

    def test_single_payment_passthrough(self, spark):
        """Single payment per order should pass through unchanged."""
        data = [
            ("order_1", "boleto", 250.0, 1),
        ]
        df = spark.createDataFrame(data, ["order_id", "payment_type", "payment_value", "payment_installments"])

        result = aggregate_order_payments(df)
        row = result.collect()[0]
        assert row.total_payment_value == 250.0
        assert row.primary_payment_type == "boleto"
        assert row.max_installments == 1


class TestAggregateOrderItems:
    """Tests the real aggregate_order_items() function."""

    def test_item_count(self, spark):
        """item_count should reflect number of line items per order."""
        data = [
            ("order_1", 50.0, 10.0),
            ("order_1", 30.0, 8.0),
            ("order_1", 20.0, 5.0),
            ("order_2", 100.0, 15.0),
        ]
        df = spark.createDataFrame(data, ["order_id", "price", "freight_value"])

        result = aggregate_order_items(df).orderBy("order_id")
        rows = result.collect()

        assert rows[0].item_count == 3
        assert rows[0].total_item_value == 100.0
        assert rows[0].total_freight_value == 23.0
        assert rows[1].item_count == 1

    def test_single_item_order(self, spark):
        """Single-item order should have item_count = 1."""
        data = [
            ("order_1", 99.99, 12.50),
        ]
        df = spark.createDataFrame(data, ["order_id", "price", "freight_value"])

        result = aggregate_order_items(df)
        row = result.collect()[0]
        assert row.item_count == 1
        assert row.total_item_value == 99.99
        assert row.total_freight_value == 12.50
