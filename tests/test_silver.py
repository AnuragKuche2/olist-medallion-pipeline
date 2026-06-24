# tests/test_silver.py
"""
Unit tests for Silver layer transformation logic.

These tests import and call the REAL clean_* functions from
src/silver/transform.py, ensuring that any regression in the
actual codebase is caught by the test suite.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

# Import real functions under test
from src.silver.transform import (
    clean_orders,
    clean_customers,
    clean_products,
    clean_sellers,
    clean_order_items,
    clean_order_payments,
    clean_order_reviews,
)


# ============================================================
# ORDERS
# ============================================================
class TestCleanOrders:
    """Tests the real clean_orders() function."""

    def test_deduplication(self, spark):
        """Duplicate order_ids should be removed."""
        data = [
            ("order_1", "DELIVERED", "2018-01-01 10:00:00", None, None, "2018-01-08 14:00:00", "2018-01-10 00:00:00"),
            ("order_1", "DELIVERED", "2018-01-01 10:00:00", None, None, "2018-01-08 14:00:00", "2018-01-10 00:00:00"),
            ("order_2", "SHIPPED", "2018-01-02 08:00:00", None, None, None, "2018-01-12 00:00:00"),
        ]
        df = spark.createDataFrame(data, [
            "order_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ])

        result = clean_orders(df)
        assert result.count() == 2

    def test_timestamp_casting(self, spark):
        """String timestamps should be cast to TimestampType."""
        data = [
            ("order_1", "delivered", "2018-01-01 10:00:00", "2018-01-01 12:00:00",
             "2018-01-03 08:00:00", "2018-01-08 14:00:00", "2018-01-10 00:00:00"),
        ]
        df = spark.createDataFrame(data, [
            "order_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ])

        result = clean_orders(df)
        assert result.schema["order_purchase_timestamp"].dataType == TimestampType()
        assert result.schema["order_delivered_customer_date"].dataType == TimestampType()

    def test_delivery_days_derived(self, spark):
        """delivery_days should be the diff between delivered and purchase."""
        data = [
            ("order_1", "delivered", "2018-01-01 10:00:00", None, None, "2018-01-08 14:00:00", "2018-01-10 00:00:00"),
        ]
        df = spark.createDataFrame(data, [
            "order_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ])

        result = clean_orders(df)
        row = result.collect()[0]
        assert row.delivery_days == 7

    def test_is_late_delivery(self, spark):
        """is_late_delivery should be True when delivered > estimated."""
        data = [
            ("order_1", "delivered", "2018-01-01 10:00:00", None, None, "2018-01-15 14:00:00", "2018-01-10 00:00:00"),
            ("order_2", "delivered", "2018-01-01 10:00:00", None, None, "2018-01-05 14:00:00", "2018-01-10 00:00:00"),
        ]
        df = spark.createDataFrame(data, [
            "order_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ])

        result = clean_orders(df).orderBy("order_id")
        rows = result.collect()
        assert rows[0].is_late_delivery is True   # delivered Jan 15 > estimated Jan 10
        assert rows[1].is_late_delivery is False  # delivered Jan 5 < estimated Jan 10

    def test_status_lowercased(self, spark):
        """order_status should be lowercased."""
        data = [
            ("order_1", "DELIVERED", "2018-01-01 10:00:00", None, None, "2018-01-08 14:00:00", "2018-01-10 00:00:00"),
        ]
        df = spark.createDataFrame(data, [
            "order_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ])

        result = clean_orders(df)
        assert result.collect()[0].order_status == "delivered"

    def test_bronze_metadata_dropped(self, spark):
        """Bronze metadata columns should be removed."""
        data = [
            ("order_1", "delivered", "2018-01-01 10:00:00", None, None, None, "2018-01-10 00:00:00",
             "2024-01-01T00:00:00", "file.csv", "batch-1"),
        ]
        df = spark.createDataFrame(data, [
            "order_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
            "_ingestion_timestamp", "_source_file", "_batch_id",
        ])

        result = clean_orders(df)
        assert "_ingestion_timestamp" not in result.columns
        assert "_source_file" not in result.columns
        assert "_batch_id" not in result.columns


# ============================================================
# CUSTOMERS
# ============================================================
class TestCleanCustomers:
    """Tests the real clean_customers() function."""

    def test_deduplication(self, spark):
        """Duplicate customer_ids should be removed."""
        data = [
            ("cust_1", "uid_1", "01234", "  São Paulo  ", "sp"),
            ("cust_1", "uid_1", "01234", "  São Paulo  ", "sp"),
            ("cust_2", "uid_2", "56789", "Rio de Janeiro", "rj"),
        ]
        df = spark.createDataFrame(data, [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ])

        result = clean_customers(df)
        assert result.count() == 2

    def test_city_standardized(self, spark):
        """City should be lowercased and trimmed."""
        data = [
            ("cust_1", "uid_1", "01234", "  SÃO PAULO  ", "SP"),
        ]
        df = spark.createDataFrame(data, [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ])

        result = clean_customers(df)
        assert result.collect()[0].customer_city == "são paulo"

    def test_state_uppercased(self, spark):
        """State should be uppercased."""
        data = [
            ("cust_1", "uid_1", "01234", "city", "sp"),
        ]
        df = spark.createDataFrame(data, [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ])

        result = clean_customers(df)
        assert result.collect()[0].customer_state == "SP"

    def test_region_derived(self, spark):
        """Region should be derived from state (SP → Sudeste)."""
        data = [
            ("cust_1", "uid_1", "01234", "city_a", "SP"),
            ("cust_2", "uid_2", "56789", "city_b", "BA"),
        ]
        df = spark.createDataFrame(data, [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ])

        result = clean_customers(df).orderBy("customer_id")
        rows = result.collect()
        assert rows[0].region == "Sudeste"
        assert rows[1].region == "Nordeste"

    def test_zip_padded(self, spark):
        """Zip codes should be left-padded to 5 digits."""
        data = [
            ("cust_1", "uid_1", "123", "city", "SP"),
        ]
        df = spark.createDataFrame(data, [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ])

        result = clean_customers(df)
        assert result.collect()[0].customer_zip_code_prefix == "00123"


# ============================================================
# PRODUCTS
# ============================================================
class TestCleanProducts:
    """Tests the real clean_products() function."""

    def test_deduplication(self, spark):
        """Duplicate product_ids should be removed."""
        data = [
            ("prod_1", "electronics", "30", "200", "3", "1500", "30", "20", "15"),
            ("prod_1", "electronics", "30", "200", "3", "1500", "30", "20", "15"),
            ("prod_2", "furniture", "20", "100", "2", "5000", "50", "40", "30"),
        ]
        df = spark.createDataFrame(data, [
            "product_id", "product_category_name",
            "product_name_lenght", "product_description_lenght",
            "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ])

        result = clean_products(df)
        assert result.count() == 2

    def test_weight_kg_derived(self, spark):
        """product_weight_kg should be weight_g / 1000."""
        data = [
            ("prod_1", "electronics", "30", "200", "3", "1500", "30", "20", "15"),
        ]
        df = spark.createDataFrame(data, [
            "product_id", "product_category_name",
            "product_name_lenght", "product_description_lenght",
            "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ])

        result = clean_products(df)
        assert result.collect()[0].product_weight_kg == 1.5

    def test_volume_derived(self, spark):
        """product_volume_cm3 should be length × height × width."""
        data = [
            ("prod_1", "electronics", "30", "200", "3", "1500", "10", "20", "30"),
        ]
        df = spark.createDataFrame(data, [
            "product_id", "product_category_name",
            "product_name_lenght", "product_description_lenght",
            "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ])

        result = clean_products(df)
        assert result.collect()[0].product_volume_cm3 == 6000.0

    def test_null_category_filled(self, spark):
        """Null product_category_name should become 'unknown'."""
        data = [
            ("prod_1", None, "30", "200", "3", "1500", "10", "20", "30"),
        ]
        df = spark.createDataFrame(data, [
            "product_id", "product_category_name",
            "product_name_lenght", "product_description_lenght",
            "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ])

        result = clean_products(df)
        assert result.collect()[0].product_category_name == "unknown"

    def test_numeric_columns_cast(self, spark):
        """Numeric string columns should be cast to DoubleType."""
        data = [
            ("prod_1", "electronics", "30", "200", "3", "1500", "10", "20", "30"),
        ]
        df = spark.createDataFrame(data, [
            "product_id", "product_category_name",
            "product_name_lenght", "product_description_lenght",
            "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ])

        result = clean_products(df)
        assert result.schema["product_weight_g"].dataType == DoubleType()
        assert result.schema["product_length_cm"].dataType == DoubleType()
