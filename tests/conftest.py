# tests/conftest.py
"""
Pytest fixtures for Spark-based unit tests.

Provides a shared SparkSession that:
  - Runs locally (no cluster needed)
  - Uses Delta Lake
  - Is reused across all tests in a session (fast)
  - Writes to a temp directory (cleaned up after)
"""

import pytest
import tempfile
import shutil
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("UnitTests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def tmp_path_delta(tmp_path):
    """Provide a temp directory for Delta table writes."""
    delta_dir = tmp_path / "delta"
    delta_dir.mkdir()
    return str(delta_dir)
