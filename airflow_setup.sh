#!/bin/bash
# airflow_setup.sh
# Quick Airflow setup for EC2 (t3.large, lightweight config)
#
# Usage: bash airflow_setup.sh
#
# This sets up Airflow with:
#   - SQLite backend (no Postgres needed — fine for single-user dev)
#   - LocalExecutor → SequentialExecutor (SQLite limitation, sufficient for this project)
#   - Minimal memory footprint (~200MB)
#   - DAGs folder pointing to project dags/

set -e

echo "============================================"
echo "  Airflow Setup — Olist Medallion Pipeline"
echo "============================================"

# --- Install Airflow ---
echo ""
echo "📦 Installing Apache Airflow..."
pip3 install --user "apache-airflow==2.8.1" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.9.txt"

# --- Set AIRFLOW_HOME ---
export AIRFLOW_HOME=~/airflow
echo "export AIRFLOW_HOME=~/airflow" >> ~/.bashrc

# --- Initialize DB ---
echo ""
echo "🗄️  Initializing Airflow database..."
~/.local/bin/airflow db init

# --- Configure ---
echo ""
echo "⚙️  Configuring Airflow..."

# Point DAGs to project folder
sed -i "s|dags_folder = .*|dags_folder = /home/ssm-user/olist-medallion-pipeline/dags|" $AIRFLOW_HOME/airflow.cfg

# Reduce resource usage
sed -i "s|parallelism = .*|parallelism = 4|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|max_active_tasks_per_dag = .*|max_active_tasks_per_dag = 2|" $AIRFLOW_HOME/airflow.cfg
sed -i "s|max_active_runs_per_dag = .*|max_active_runs_per_dag = 1|" $AIRFLOW_HOME/airflow.cfg

# Disable examples
sed -i "s|load_examples = .*|load_examples = False|" $AIRFLOW_HOME/airflow.cfg

# --- Create Admin User ---
echo ""
echo "👤 Creating admin user..."
~/.local/bin/airflow users create \
    --username admin \
    --firstname Anurag \
    --lastname Kuche \
    --role Admin \
    --email anukuche@amazon.com \
    --password admin123

echo ""
echo "============================================"
echo "  ✅ Airflow Setup Complete!"
echo "============================================"
echo ""
echo "  To start Airflow:"
echo "    ~/.local/bin/airflow standalone"
echo ""
echo "  Or run scheduler + webserver separately:"
echo "    ~/.local/bin/airflow scheduler -D"
echo "    ~/.local/bin/airflow webserver -p 8080 -D"
echo ""
echo "  Web UI: http://localhost:8080"
echo "  Login:  admin / admin123"
echo ""
echo "  To trigger the pipeline manually:"
echo "    ~/.local/bin/airflow dags trigger olist_medallion_pipeline"
echo ""
echo "============================================"
