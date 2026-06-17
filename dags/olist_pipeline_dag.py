# dags/olist_pipeline_dag.py
"""
Olist Medallion Pipeline — Airflow DAG.

Orchestrates the full pipeline: Bronze → Silver → Gold.

Design choices:
  - BashOperator with python3 -m: Simple, uses existing module structure
  - Linear dependency: Bronze must complete before Silver, Silver before Gold
  - Retries: 2 retries with 5-min delay for transient S3 issues
  - Schedule: Daily at 6 AM UTC (can be event-triggered later)
  - Tags: For filtering in Airflow UI

Phase 2 upgrades:
  - SparkSubmitOperator (for EMR/Databricks)
  - Sensors for file arrival
  - Data quality gates between layers
  - Slack/email alerting on failure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


# ============================================================
# DAG CONFIGURATION
# ============================================================
default_args = {
    "owner": "anukuche",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

PROJECT_DIR = "/home/ssm-user/olist-medallion-pipeline"

# Environment variables for Spark
SPARK_ENV = (
    "export SPARK_HOME=/opt/spark && "
    "export PATH=$SPARK_HOME/bin:$PATH && "
    "export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*.zip):$PYTHONPATH"
)


# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id="olist_medallion_pipeline",
    default_args=default_args,
    description="Olist E-Commerce Medallion Architecture: Landing → Bronze → Silver → Gold",
    schedule_interval="0 6 * * *",  # Daily at 6 AM UTC
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["medallion", "olist", "data-engineering"],
    doc_md="""
    ## Olist Medallion Pipeline
    
    Full ETL pipeline processing Brazilian e-commerce data through:
    - **Bronze**: Raw CSV → Delta Lake with schema enforcement + quarantine
    - **Silver**: Cleaning, dedup, type casting, derived columns
    - **Gold**: Star schema (5 dimensions + 2 fact tables)
    
    **Data Source**: Olist Brazilian E-Commerce (Kaggle)  
    **Output**: Star schema in S3 (Delta Lake format)
    """,
) as dag:

    # --- Start ---
    start = EmptyOperator(
        task_id="start",
    )

    # --- Bronze Layer ---
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"{SPARK_ENV} && cd {PROJECT_DIR} && python3 -m src.bronze.ingest",
        doc_md="Ingest all 9 CSVs from S3 landing → Bronze (Delta Lake). "
               "Applies schema enforcement, captures corrupt records to quarantine.",
    )

    # --- Silver Layer ---
    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=f"{SPARK_ENV} && cd {PROJECT_DIR} && python3 -m src.silver.transform",
        doc_md="Transform Bronze → Silver. Applies deduplication, type casting, "
               "standardization, derived columns, and data quality flags.",
    )

    # --- Gold Layer ---
    gold_build = BashOperator(
        task_id="gold_build",
        bash_command=f"{SPARK_ENV} && cd {PROJECT_DIR} && python3 -m src.gold.build",
        doc_md="Build Gold star schema from Silver. Creates 5 dimension tables "
               "and 2 fact tables optimized for analytical queries.",
    )

    # --- Data Quality Check (placeholder) ---
    quality_check = BashOperator(
        task_id="quality_check",
        bash_command=f"{SPARK_ENV} && cd {PROJECT_DIR} && python3 -m src.quality.validate || true",
        doc_md="Run data quality validations. Currently a placeholder — "
               "will be implemented with Great Expectations or custom checks.",
    )

    # --- End ---
    end = EmptyOperator(
        task_id="end",
    )

    # ============================================================
    # DEPENDENCIES (linear pipeline)
    # ============================================================
    start >> bronze_ingest >> silver_transform >> gold_build >> quality_check >> end
