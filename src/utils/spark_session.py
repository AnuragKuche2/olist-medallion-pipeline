# src/utils/spark_session.py
"""
Reusable Spark session factory for the Olist Medallion Pipeline.
Configures Delta Lake, S3 access, and memory settings.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "OlistMedallionPipeline") -> SparkSession:
    """
    Creates or gets a configured SparkSession with Delta Lake and S3 support.

    Args:
        app_name: Name of the Spark application (visible in Spark UI)

    Returns:
        Configured SparkSession
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Memory settings (tuned for t3.large — 8GB RAM)
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "8")
        # Delta Lake optimizations
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    return spark
