# Olist Medallion Pipeline

> End-to-end Medallion Architecture data pipeline processing 1.5M+ Brazilian e-commerce records through Bronze → Silver → Gold layers with Delta Lake, PySpark, Airflow orchestration, and automated data quality validation.

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           OLIST MEDALLION PIPELINE                                      │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│   ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐      │
│   │  SOURCE  │────▶│  BRONZE  │────▶│  SILVER  │────▶│   GOLD   │────▶│ ANALYTICS│      │
│   │  (CSV)   │     │  (Delta) │     │  (Delta) │     │  (Delta) │     │   / BI   │      │
│   └──────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘      │
│                           │                                                             │
│                           ▼                                                             │
│                    ┌──────────────┐                                                     │
│                    │  QUARANTINE  │                                                     │
│                    │  (bad records)│                                                    │
│                    └──────────────┘                                                     │
│                                                                                         │
│   Storage: AWS S3 (s3a://)          Compute: PySpark 3.5 on EC2                         │
│   Format: Delta Lake 3.1            Orchestration: Apache Airflow                       │
│   Quality: Custom validation suite  Testing: pytest (27 tests)                          │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🏗️ Layer Design

| Layer | Purpose | Records | Format | Key Operations |
|-------|---------|---------|--------|----------------|
| **Landing** | Raw source files | 1,550,922 | CSV | None — files as-is from Kaggle |
| **Bronze** | Raw → structured with metadata | 1,555,860 | Delta Lake | Schema enforcement, corrupt record quarantine, audit metadata |
| **Silver** | Cleaned, validated, conformed | 565,091 | Delta Lake | Dedup, type casting, standardization, derived columns |
| **Gold** | Business-ready star schema | 364,244 | Delta Lake | Dimensional modeling (5 dims + 2 facts) |

### Bronze Layer — "Trust but verify"
- **Explicit schemas** (no `inferSchema`) — 10x faster, safer, self-documenting
- **PERMISSIVE mode** with `_corrupt_record` column — production pipelines don't crash
- **Quarantine pattern** — bad records isolated for investigation
- **Audit metadata** — `_ingestion_timestamp`, `_source_file`, `_batch_id` on every record
- **Generic ingestion** — one config-driven function handles all 9 tables (DRY)

### Silver Layer — "Single source of truth"
- **Deduplication** — order_reviews: 9,621 duplicates removed; geolocation: 1M → 19K unique zips
- **Type casting** — strings → timestamps, proper numerics, padded zip codes
- **Standardization** — lowercase cities, uppercase states, consistent formats
- **Derived columns** — `delivery_days`, `is_late_delivery`, `weight_kg`, `volume_cm3`, `region`
- **Data quality flags** — `_dq_valid_price` for downstream filtering

### Gold Layer — Star Schema
```
                         ┌───────────┐
                         │ dim_date  │
                         │ (774 rows)│
                         └─────┬─────┘
                               │
┌─────────────┐    ┌───────────┴───────────┐    ┌──────────────┐
│dim_customer │────│      fact_orders      │    │dim_geography │
│(96,096 rows)│    │    (99,441 rows)      │    │(19,015 rows) │
└─────────────┘    └───────────┬───────────┘    └──────────────┘
                               │
                    ┌──────────┴──────────┐
                    │  fact_order_items   │
                    │   (112,650 rows)    │
                    └─────┬─────────┬────┘
                          │         │
              ┌───────────┴┐   ┌───┴───────────┐
              │dim_product │   │  dim_seller   │
              │(32,951 rows)│   │ (3,095 rows) │
              └────────────┘   └───────────────┘
```

**Why star schema over snowflake?**
- Fewer joins for analytical queries (2-3 vs 5+)
- Dimensions are small (3K-99K rows) — further normalization saves negligible storage
- BI tools (QuickSight, Tableau) optimize for star schemas

---

## 📊 Data Source

**Olist Brazilian E-Commerce Dataset** — real, anonymized commercial data from Brazil's largest marketplace.

| Table | Records | Description |
|-------|---------|-------------|
| orders | 99,441 | Central table — order header |
| order_items | 112,650 | Line items per order |
| order_payments | 103,886 | Payment methods/installments |
| order_reviews | 99,224 | Customer review scores |
| customers | 99,441 | Customer demographics |
| products | 32,951 | Product catalog |
| sellers | 3,095 | Seller information |
| geolocation | 1,000,163 | Zip → lat/lng mapping |
| category_translation | 71 | Portuguese → English categories |

**Coverage:** ~100,000 orders from Sept 2016 to Oct 2018 across 27 Brazilian states.

---

## 🛠️ Tech Stack

| Tool | Purpose | Why This Choice |
|------|---------|-----------------|
| **PySpark 3.5** | Distributed data processing | Industry standard for large-scale ETL |
| **Delta Lake 3.1** | ACID table format | Time travel, schema evolution, MERGE support |
| **AWS S3** | Data lake storage (all layers) | Scalable, cheap, decoupled from compute |
| **AWS EC2** (t3.large) | Compute | 8GB RAM sufficient for Phase 1 (140MB) |
| **Apache Airflow** | Orchestration | DAG dependencies, scheduling, monitoring |
| **pytest** | Unit testing | 27 tests validating transformation logic |
| **AWS IAM** | Security | Role-based S3 access (no hardcoded keys) |
| **Git/GitHub** | Version control | Conventional commits, clean history |

---

## 📁 Project Structure

```
olist-medallion-pipeline/
├── src/
│   ├── utils/
│   │   ├── spark_session.py          # Reusable Spark session factory
│   │   └── schema_definitions.py     # Explicit schemas for all 9 tables
│   ├── bronze/
│   │   ├── ingest.py                 # Generic ingestion (config-driven, DRY)
│   │   └── ingest_orders.py          # Single-table example
│   ├── silver/
│   │   └── transform.py             # Per-table cleaning + shared utilities
│   ├── gold/
│   │   └── build.py                 # Star schema builder (dims first, then facts)
│   └── quality/
│       └── validate.py              # 39 data quality checks
├── dags/
│   └── olist_pipeline_dag.py        # Airflow DAG (Bronze → Silver → Gold → QA)
├── tests/
│   ├── conftest.py                  # Shared Spark fixture (local mode)
│   ├── test_bronze.py               # Schema enforcement, metadata tests
│   ├── test_silver.py               # Dedup, casting, standardization tests
│   └── test_gold.py                 # Dim/fact logic, surrogate keys tests
├── airflow_setup.sh                 # One-command Airflow setup for EC2
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- AWS account with S3 access
- EC2 instance (t3.large or larger) with IAM role for S3
- Python 3.9+, Java 11+, Spark 3.5

### Setup
```bash
# Clone
git clone https://github.com/AnuragKuche2/olist-medallion-pipeline.git
cd olist-medallion-pipeline

# Install dependencies
pip3 install --user -r requirements.txt

# Set Spark environment
export SPARK_HOME=/opt/spark
export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*.zip):$PYTHONPATH
```

### Run the Pipeline
```bash
# Full pipeline (Bronze → Silver → Gold):
python3 -m src.bronze.ingest
python3 -m src.silver.transform
python3 -m src.gold.build

# Data quality validation:
python3 -m src.quality.validate

# Unit tests:
python3 -m pytest tests/ -v

# Airflow (orchestrated):
bash airflow_setup.sh
~/.local/bin/airflow dags trigger olist_medallion_pipeline
```

---

## ✅ Data Quality

**39 automated checks** run after every pipeline execution:

| Category | Checks | What's Validated |
|----------|--------|------------------|
| Row Counts | 10 | Silver ≤ Bronze, no unexpected gain/loss |
| Null Checks | 12 | Critical columns (PKs, FKs, prices) < 5% null |
| Referential Integrity | 4 | Fact keys exist in dimension tables |
| Value Ranges | 4 | Prices > 0, scores 1-5, delivery 0-120 days, Brazil coordinates |
| Uniqueness | 9 | Primary keys are unique across Silver and Gold |

**Current result:** 38 PASS, 1 WARNING (43 orders with delivery > 120 days — legitimate outliers)

---

## 🧪 Testing

```bash
$ python3 -m pytest tests/ -v
============================= 27 passed in 28.04s =============================
```

Tests run on **local Spark** (no S3 dependency) with in-memory DataFrames, validating:
- Schema enforcement and corrupt record separation
- Deduplication, type casting, null handling
- City/state standardization, zip padding
- Derived column calculations (delivery_days, is_late, weight_kg, volume)
- Seller tier classification
- Surrogate key uniqueness
- Fact table aggregation patterns

---

## 🔑 Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Schema approach | Explicit (not inferSchema) | 10x faster, prevents silent type drift, documents structure |
| Read mode | PERMISSIVE + corrupt record | Production resilience — don't crash for one bad row |
| Write mode | Overwrite (Phase 1) | Full refresh now; MERGE INTO for incremental later |
| S3 protocol | `s3a://` | Hadoop's modern connector — supports multipart uploads, streaming, IAM roles |
| Data model | Star (not snowflake) | Fewer joins, small dimensions, BI-tool optimized |
| Bronze pattern | Generic/config-driven | DRY — one function handles all 9 tables |
| EC2 access | SSM Session Manager | More secure than SSH keys, IAM-controlled |
| Geolocation dedup | Aggregate to zip prefix | 1M → 19K — one row per zip with avg coordinates |
| Seller enrichment | Compute metrics in Gold | avg_review_score + seller_tier derived from Silver |

---

## 📈 Performance

| Metric | Phase 1 |
|--------|---------|
| Total source data | 140MB (9 CSVs, 1.55M rows) |
| Bronze ingestion | ~3 minutes |
| Silver transformation | ~4 minutes |
| Gold build | ~5 minutes |
| Full pipeline | ~12 minutes end-to-end |
| Quality validation | ~3 minutes (39 checks) |
| Unit tests | 28 seconds |

---

## 🗺️ Roadmap (Phase 2)

| Feature | Purpose |
|---------|---------|
| **Scale to 50-80GB** (Faker) | Demonstrate real distributed processing + partition strategy |
| **dbt on Databricks** | SQL-based Gold modeling with refs, tests, lineage |
| **Spark on EKS** | Auto-scaling K8s cluster for production workloads |
| **Streaming ingestion** | Auto Loader / Structured Streaming for incremental |
| **MERGE INTO** | Idempotent upserts (replace overwrite) |
| **Delta OPTIMIZE + Z-ORDER** | File compaction + co-location for query performance |
| **CI/CD** | Automated testing on push via GitHub Actions |
| **SCD Type 2** | Slowly Changing Dimensions for historical tracking |

---

## 💰 Cost

| Resource | Monthly Estimate |
|----------|-----------------|
| EC2 t3.large (5 hrs/day × 21 days) | ~$8-12 |
| S3 storage (140MB) | ~$0.02 |
| EBS (50GB gp3) | ~$4 |
| **Total** | **~$15-20** |

---

## 📄 License

This project is for educational/assessment purposes. Data source: [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (CC BY-NC-SA 4.0).

---

## 👤 Author

**Anurag Kuche** — AWS Data Builder  
GitHub: [@AnuragKuche2](https://github.com/AnuragKuche2)
