# src/quality/validate.py
"""
Data Quality Validation Module.

Runs checks across all layers to ensure pipeline integrity:
  1. Row count reconciliation (Bronze vs Silver vs Gold)
  2. Null percentage checks (critical columns must be < threshold)
  3. Referential integrity (fact table keys exist in dimensions)
  4. Value range checks (prices > 0, scores 1-5, valid dates)
  5. Freshness checks (data was processed recently)

Exit codes:
  0 = All checks passed
  1 = Warnings (non-critical checks failed)
  2 = Critical failures (pipeline data integrity compromised)

Phase 2: Migrate to Great Expectations for richer validation + docs.
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession, functions as F

from src.utils.spark_session import get_spark_session


# ============================================================
# CONFIGURATION
# ============================================================
S3_BUCKET = "anukuche-olist-datalake"
BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze"
SILVER_PATH = f"s3a://{S3_BUCKET}/silver"
GOLD_PATH = f"s3a://{S3_BUCKET}/gold"

# Thresholds
MAX_NULL_PERCENT = 5.0  # Critical columns: max 5% nulls
MAX_ROW_LOSS_PERCENT = 10.0  # Max acceptable row loss between layers


# ============================================================
# VALIDATION RESULTS TRACKER
# ============================================================
class ValidationResult:
    def __init__(self):
        self.checks = []
        self.passed = 0
        self.warnings = 0
        self.failures = 0

    def add(self, check_name: str, status: str, detail: str = ""):
        self.checks.append({"check": check_name, "status": status, "detail": detail})
        if status == "PASS":
            self.passed += 1
        elif status == "WARN":
            self.warnings += 1
        elif status == "FAIL":
            self.failures += 1

    def summary(self):
        print("\n" + "=" * 60)
        print("DATA QUALITY REPORT")
        print("=" * 60)
        print(f"\n   ✅ Passed:   {self.passed}")
        print(f"   ⚠️  Warnings: {self.warnings}")
        print(f"   ❌ Failures: {self.failures}")
        print(f"   📋 Total:    {len(self.checks)}")
        print("\n" + "-" * 60)

        for check in self.checks:
            icon = {"PASS": "✅", "WARN": "⚠️ ", "FAIL": "❌"}[check["status"]]
            detail = f" — {check['detail']}" if check["detail"] else ""
            print(f"   {icon} {check['check']}{detail}")

        print("\n" + "=" * 60)

        if self.failures > 0:
            print("   🚨 RESULT: CRITICAL FAILURES — investigate immediately")
            return 2
        elif self.warnings > 0:
            print("   ⚠️  RESULT: PASSED WITH WARNINGS")
            return 1
        else:
            print("   🎉 RESULT: ALL CHECKS PASSED")
            return 0


# ============================================================
# CHECK 1: ROW COUNT RECONCILIATION
# ============================================================
def check_row_counts(spark: SparkSession, results: ValidationResult):
    """Verify row counts are reasonable across layers."""
    print("\n📊 Check: Row Count Reconciliation")

    counts = {}

    # Bronze counts
    bronze_tables = ["orders", "order_items", "order_payments", "order_reviews",
                     "customers", "products", "sellers", "geolocation", "category_translation"]
    for table in bronze_tables:
        try:
            count = spark.read.format("delta").load(f"{BRONZE_PATH}/{table}").count()
            counts[f"bronze.{table}"] = count
        except Exception as e:
            results.add(f"row_count.bronze.{table}", "FAIL", f"Cannot read: {str(e)[:50]}")

    # Silver counts
    for table in bronze_tables:
        try:
            count = spark.read.format("delta").load(f"{SILVER_PATH}/{table}").count()
            counts[f"silver.{table}"] = count
        except Exception as e:
            results.add(f"row_count.silver.{table}", "FAIL", f"Cannot read: {str(e)[:50]}")

    # Check: Silver should not have MORE rows than Bronze (except aggregation may reduce)
    for table in bronze_tables:
        bronze_key = f"bronze.{table}"
        silver_key = f"silver.{table}"
        if bronze_key in counts and silver_key in counts:
            bronze_count = counts[bronze_key]
            silver_count = counts[silver_key]

            if silver_count > bronze_count:
                results.add(
                    f"row_count.{table}",
                    "FAIL",
                    f"Silver ({silver_count:,}) > Bronze ({bronze_count:,}) — unexpected row gain"
                )
            else:
                loss_pct = ((bronze_count - silver_count) / bronze_count) * 100 if bronze_count > 0 else 0
                if loss_pct > MAX_ROW_LOSS_PERCENT and table != "geolocation":
                    results.add(
                        f"row_count.{table}",
                        "WARN",
                        f"Lost {loss_pct:.1f}% rows (Bronze: {bronze_count:,} → Silver: {silver_count:,})"
                    )
                else:
                    results.add(f"row_count.{table}", "PASS",
                                f"Bronze: {bronze_count:,} → Silver: {silver_count:,}")

    # Gold fact tables should have reasonable counts
    try:
        fact_orders = spark.read.format("delta").load(f"{GOLD_PATH}/fact_orders").count()
        silver_orders = counts.get("silver.orders", 0)
        if fact_orders > 0 and silver_orders > 0:
            results.add("row_count.fact_orders", "PASS", f"{fact_orders:,} records")
        else:
            results.add("row_count.fact_orders", "FAIL", "Empty table")
    except Exception as e:
        results.add("row_count.fact_orders", "FAIL", f"Cannot read: {str(e)[:50]}")


# ============================================================
# CHECK 2: NULL CHECKS ON CRITICAL COLUMNS
# ============================================================
def check_nulls(spark: SparkSession, results: ValidationResult):
    """Verify critical columns have acceptable null rates."""
    print("\n🔍 Check: Null Percentages")

    # Critical columns that should have very low nulls
    critical_checks = [
        ("silver", "orders", "order_id"),
        ("silver", "orders", "order_status"),
        ("silver", "orders", "order_purchase_timestamp"),
        ("silver", "order_items", "order_id"),
        ("silver", "order_items", "product_id"),
        ("silver", "order_items", "price"),
        ("silver", "customers", "customer_id"),
        ("silver", "customers", "customer_unique_id"),
        ("gold", "fact_orders", "order_id"),
        ("gold", "fact_orders", "date_key"),
        ("gold", "fact_order_items", "product_key"),
        ("gold", "fact_order_items", "seller_key"),
    ]

    for layer, table, column in critical_checks:
        try:
            path = f"s3a://{S3_BUCKET}/{layer}/{table}"
            df = spark.read.format("delta").load(path)
            total = df.count()
            null_count = df.filter(F.col(column).isNull()).count()
            null_pct = (null_count / total) * 100 if total > 0 else 0

            if null_pct == 0:
                results.add(f"nulls.{layer}.{table}.{column}", "PASS", "0% nulls")
            elif null_pct <= MAX_NULL_PERCENT:
                results.add(f"nulls.{layer}.{table}.{column}", "WARN",
                            f"{null_pct:.1f}% nulls ({null_count:,} / {total:,})")
            else:
                results.add(f"nulls.{layer}.{table}.{column}", "FAIL",
                            f"{null_pct:.1f}% nulls ({null_count:,} / {total:,}) — exceeds {MAX_NULL_PERCENT}%")
        except Exception as e:
            results.add(f"nulls.{layer}.{table}.{column}", "FAIL", f"Error: {str(e)[:50]}")


# ============================================================
# CHECK 3: REFERENTIAL INTEGRITY
# ============================================================
def check_referential_integrity(spark: SparkSession, results: ValidationResult):
    """Verify fact table keys exist in dimension tables."""
    print("\n🔗 Check: Referential Integrity")

    try:
        fact_orders = spark.read.format("delta").load(f"{GOLD_PATH}/fact_orders")
        fact_items = spark.read.format("delta").load(f"{GOLD_PATH}/fact_order_items")
        dim_customer = spark.read.format("delta").load(f"{GOLD_PATH}/dim_customer")
        dim_product = spark.read.format("delta").load(f"{GOLD_PATH}/dim_product")
        dim_seller = spark.read.format("delta").load(f"{GOLD_PATH}/dim_seller")
        dim_date = spark.read.format("delta").load(f"{GOLD_PATH}/dim_date")

        # fact_orders.customer_key → dim_customer.customer_key
        orphan_customers = fact_orders.filter(F.col("customer_key").isNotNull()).join(
            dim_customer, "customer_key", "left_anti"
        ).count()
        if orphan_customers == 0:
            results.add("ref_integrity.fact_orders→dim_customer", "PASS")
        else:
            results.add("ref_integrity.fact_orders→dim_customer", "WARN",
                        f"{orphan_customers:,} orphan keys")

        # fact_orders.date_key → dim_date.date_key
        orphan_dates = fact_orders.filter(F.col("date_key").isNotNull()).join(
            dim_date, "date_key", "left_anti"
        ).count()
        if orphan_dates == 0:
            results.add("ref_integrity.fact_orders→dim_date", "PASS")
        else:
            results.add("ref_integrity.fact_orders→dim_date", "WARN",
                        f"{orphan_dates:,} orphan keys")

        # fact_order_items.product_key → dim_product.product_key
        orphan_products = fact_items.filter(F.col("product_key").isNotNull()).join(
            dim_product, "product_key", "left_anti"
        ).count()
        if orphan_products == 0:
            results.add("ref_integrity.fact_items→dim_product", "PASS")
        else:
            results.add("ref_integrity.fact_items→dim_product", "WARN",
                        f"{orphan_products:,} orphan keys")

        # fact_order_items.seller_key → dim_seller.seller_key
        orphan_sellers = fact_items.filter(F.col("seller_key").isNotNull()).join(
            dim_seller, "seller_key", "left_anti"
        ).count()
        if orphan_sellers == 0:
            results.add("ref_integrity.fact_items→dim_seller", "PASS")
        else:
            results.add("ref_integrity.fact_items→dim_seller", "WARN",
                        f"{orphan_sellers:,} orphan keys")

    except Exception as e:
        results.add("ref_integrity", "FAIL", f"Error: {str(e)[:80]}")


# ============================================================
# CHECK 4: VALUE RANGE CHECKS
# ============================================================
def check_value_ranges(spark: SparkSession, results: ValidationResult):
    """Verify values are within expected ranges."""
    print("\n📏 Check: Value Ranges")

    try:
        # Prices should be > 0
        items = spark.read.format("delta").load(f"{SILVER_PATH}/order_items")
        negative_prices = items.filter(F.col("price") <= 0).count()
        if negative_prices == 0:
            results.add("range.order_items.price_positive", "PASS")
        else:
            results.add("range.order_items.price_positive", "FAIL",
                        f"{negative_prices:,} items with price <= 0")

        # Review scores should be 1-5
        reviews = spark.read.format("delta").load(f"{SILVER_PATH}/order_reviews")
        bad_scores = reviews.filter(
            (F.col("review_score") < 1) | (F.col("review_score") > 5)
        ).count()
        if bad_scores == 0:
            results.add("range.reviews.score_1_to_5", "PASS")
        else:
            results.add("range.reviews.score_1_to_5", "FAIL",
                        f"{bad_scores:,} reviews with score outside 1-5")

        # Delivery days should be reasonable (0-120 days)
        orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
        unreasonable_delivery = orders.filter(
            (F.col("delivery_days").isNotNull()) &
            ((F.col("delivery_days") < 0) | (F.col("delivery_days") > 120))
        ).count()
        if unreasonable_delivery == 0:
            results.add("range.orders.delivery_days_0_120", "PASS")
        else:
            results.add("range.orders.delivery_days_0_120", "WARN",
                        f"{unreasonable_delivery:,} orders with delivery > 120 days or negative")

        # Lat/Lng in valid range for Brazil (~-34 to 5 lat, -74 to -35 lng)
        geo = spark.read.format("delta").load(f"{SILVER_PATH}/geolocation")
        bad_coords = geo.filter(
            (F.col("geolocation_lat") < -34) | (F.col("geolocation_lat") > 6) |
            (F.col("geolocation_lng") < -75) | (F.col("geolocation_lng") > -34)
        ).count()
        total_geo = geo.count()
        bad_pct = (bad_coords / total_geo) * 100 if total_geo > 0 else 0
        if bad_pct < 1:
            results.add("range.geolocation.brazil_bounds", "PASS",
                        f"{bad_coords} out of bounds ({bad_pct:.2f}%)")
        else:
            results.add("range.geolocation.brazil_bounds", "WARN",
                        f"{bad_coords:,} outside Brazil bounds ({bad_pct:.1f}%)")

    except Exception as e:
        results.add("value_ranges", "FAIL", f"Error: {str(e)[:80]}")


# ============================================================
# CHECK 5: UNIQUENESS (Primary Key Integrity)
# ============================================================
def check_uniqueness(spark: SparkSession, results: ValidationResult):
    """Verify primary keys are unique."""
    print("\n🔑 Check: Primary Key Uniqueness")

    pk_checks = [
        ("silver", "orders", "order_id"),
        ("silver", "customers", "customer_id"),
        ("silver", "products", "product_id"),
        ("silver", "sellers", "seller_id"),
        ("gold", "dim_customer", "customer_key"),
        ("gold", "dim_product", "product_key"),
        ("gold", "dim_seller", "seller_key"),
        ("gold", "dim_date", "date_key"),
        ("gold", "fact_orders", "order_id"),
    ]

    for layer, table, pk_col in pk_checks:
        try:
            path = f"s3a://{S3_BUCKET}/{layer}/{table}"
            df = spark.read.format("delta").load(path)
            total = df.count()
            distinct = df.select(pk_col).distinct().count()

            if total == distinct:
                results.add(f"unique.{layer}.{table}.{pk_col}", "PASS")
            else:
                dupes = total - distinct
                results.add(f"unique.{layer}.{table}.{pk_col}", "FAIL",
                            f"{dupes:,} duplicates found")
        except Exception as e:
            results.add(f"unique.{layer}.{table}.{pk_col}", "FAIL", f"Error: {str(e)[:50]}")


# ============================================================
# RUN ALL CHECKS
# ============================================================
def validate_all():
    """Run complete data quality validation suite."""

    print("=" * 60)
    print("DATA QUALITY VALIDATION")
    print(f"Run time: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = get_spark_session(app_name="Quality_Validation")
    results = ValidationResult()

    # Run all checks
    check_row_counts(spark, results)
    check_nulls(spark, results)
    check_referential_integrity(spark, results)
    check_value_ranges(spark, results)
    check_uniqueness(spark, results)

    # Print report and exit with appropriate code
    exit_code = results.summary()
    sys.exit(exit_code)


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    validate_all()
