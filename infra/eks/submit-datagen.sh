#!/bin/bash
# infra/eks/submit-datagen.sh
# Submit the 1000x data generation job to Spark on EKS
#
# Prerequisites:
#   1. EKS cluster running: eksctl create cluster -f cluster-config.yaml
#   2. RBAC applied: kubectl apply -f spark-rbac.yaml
#   3. Docker image pushed to ECR: <account-id>.dkr.ecr.<region>.amazonaws.com/olist-spark:3.5.0
#   4. IAM role created: olist-spark-eks-role (with S3 full access)
#
# Usage: bash submit-datagen.sh [scale_factor]

set -e

SCALE=${1:-1000}
# Account ID and region are resolved dynamically from your AWS credentials.
# Override by exporting AWS_ACCOUNT_ID / AWS_REGION before running this script.
ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
REGION="${AWS_REGION:-us-west-2}"
SPARK_IMAGE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/olist-spark:3.5.0"
K8S_MASTER=$(kubectl cluster-info | grep "Kubernetes control plane" | awk '{print $NF}')
NAMESPACE="spark"
SERVICE_ACCOUNT="spark-sa"

echo "============================================"
echo "  Spark on EKS — Data Generation (${SCALE}x)"
echo "============================================"
echo ""
echo "  Cluster: ${K8S_MASTER}"
echo "  Image:   ${SPARK_IMAGE}"
echo "  Scale:   ${SCALE}x (~$(echo "${SCALE} * 1550000" | bc) records)"
echo ""

spark-submit \
  --master k8s://${K8S_MASTER} \
  --deploy-mode cluster \
  --name olist-datagen-${SCALE}x \
  --conf spark.kubernetes.namespace=${NAMESPACE} \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=${SERVICE_ACCOUNT} \
  --conf spark.kubernetes.container.image=${SPARK_IMAGE} \
  --conf spark.kubernetes.container.image.pullPolicy=Always \
  \
  --conf spark.driver.cores=2 \
  --conf spark.driver.memory=4g \
  --conf spark.executor.instances=5 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=12g \
  --conf spark.executor.memoryOverhead=2g \
  \
  --conf spark.kubernetes.driver.label.app=olist-datagen \
  --conf spark.kubernetes.executor.label.app=olist-datagen \
  --conf spark.kubernetes.driver.annotation.cluster-autoscaler.kubernetes.io/safe-to-evict=false \
  \
  --conf spark.kubernetes.node.selector.role=spark-executor \
  --conf spark.kubernetes.driver.node.selector.role=spark-driver \
  \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=3 \
  --conf spark.dynamicAllocation.maxExecutors=10 \
  --conf spark.dynamicAllocation.executorAllocationRatio=0.5 \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.endpoint=s3.us-west-2.amazonaws.com \
  \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.shuffle.partitions=500 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  \
  local:///opt/spark/work-dir/src/datagen/generate.py \
  --scale ${SCALE}

echo ""
echo "============================================"
echo "  Job submitted! Monitor with:"
echo "    kubectl get pods -n spark -w"
echo "    kubectl logs -f <driver-pod> -n spark"
echo "============================================"
