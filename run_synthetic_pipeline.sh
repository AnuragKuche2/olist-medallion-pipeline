#!/bin/bash
# run_synthetic_pipeline.sh
# Runs the FULL pipeline (Bronze → Silver → Gold) on synthetic data.
#
# The env vars tell each module to read/write from separate folders
# so synthetic runs don't clobber the original dataset.
#
# Usage:
#   bash run_synthetic_pipeline.sh          # run on existing synthetic data
#   bash run_synthetic_pipeline.sh --gen 10 # generate 10x first, then run pipeline

set -e

# Parse args
GENERATE=false
SCALE=10
if [[ "$1" == "--gen" ]]; then
    GENERATE=true
    SCALE=${2:-10}
fi

# Set paths for synthetic pipeline (isolated from production)
export LANDING_FOLDER="landing_synthetic"
export BRONZE_FOLDER="bronze_synthetic"
export SILVER_FOLDER="silver_synthetic"
export GOLD_FOLDER="gold_synthetic"

echo "============================================================"
echo "  FULL PIPELINE — Synthetic Data (${LANDING_FOLDER})"
echo "============================================================"
echo ""
echo "  Landing: s3://anukuche-olist-datalake/${LANDING_FOLDER}"
echo "  Bronze:  s3://anukuche-olist-datalake/${BRONZE_FOLDER}"
echo "  Silver:  s3://anukuche-olist-datalake/${SILVER_FOLDER}"
echo "  Gold:    s3://anukuche-olist-datalake/${GOLD_FOLDER}"
echo ""

# -----------------------------------------------
# Step 0 (optional): Generate synthetic data
# -----------------------------------------------
if [ "$GENERATE" = true ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  STEP 0: Generating ${SCALE}x synthetic data"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    python3 -m src.datagen.generate --scale ${SCALE}
    echo ""
fi

# -----------------------------------------------
# Step 1: Bronze Ingestion
# -----------------------------------------------
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  STEP 1: Bronze Ingestion"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 -m src.bronze.ingest
echo ""

# -----------------------------------------------
# Step 2: Silver Transformation
# -----------------------------------------------
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  STEP 2: Silver Transformation"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 -m src.silver.transform
echo ""

# -----------------------------------------------
# Step 3: Gold Build (Star Schema)
# -----------------------------------------------
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  STEP 3: Gold Build (Star Schema)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 -m src.gold.build
echo ""

# -----------------------------------------------
# Done
# -----------------------------------------------
echo "============================================================"
echo "  ✅ FULL PIPELINE COMPLETE — Synthetic Data"
echo "============================================================"
echo ""
echo "  Verify in S3:"
echo "    aws s3 ls s3://anukuche-olist-datalake/${GOLD_FOLDER}/ --recursive | head -20"
echo ""
