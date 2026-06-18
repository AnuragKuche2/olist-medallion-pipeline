#!/bin/bash
# infra/eks/setup-eks.sh
# Complete EKS setup for Spark on Kubernetes
#
# This script:
#   1. Creates the EKS cluster with eksctl
#   2. Creates an ECR repository for the Spark image
#   3. Creates an IAM role for IRSA (S3 access from pods)
#   4. Applies Kubernetes RBAC
#   5. Builds and pushes the Docker image
#   6. Verifies everything is ready
#
# Prerequisites:
#   - AWS CLI configured
#   - eksctl installed (brew install eksctl)
#   - kubectl installed
#   - Docker running
#
# Usage: bash setup-eks.sh
#
# Estimated time: ~20 minutes (cluster creation takes 15 min)
# Estimated cost: ~$0.10/hr (control plane) + node costs

set -e

ACCOUNT_ID="025078772864"
REGION="us-west-2"
CLUSTER_NAME="olist-spark-cluster"
ECR_REPO="olist-spark"
IAM_ROLE="olist-spark-eks-role"
NAMESPACE="spark"

echo "============================================"
echo "  EKS Setup — Olist Spark Cluster"
echo "============================================"
echo ""

# -----------------------------------------------
# Step 1: Create EKS Cluster
# -----------------------------------------------
echo "📦 Step 1: Creating EKS cluster..."
echo "   (This takes ~15 minutes)"
eksctl create cluster -f cluster-config.yaml

echo "   ✅ Cluster created"
echo ""

# -----------------------------------------------
# Step 2: Create ECR Repository
# -----------------------------------------------
echo "🐳 Step 2: Creating ECR repository..."
aws ecr create-repository \
  --repository-name ${ECR_REPO} \
  --region ${REGION} \
  --image-scanning-configuration scanOnPush=true \
  2>/dev/null || echo "   (Repository already exists)"

echo "   ✅ ECR repo: ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO}"
echo ""

# -----------------------------------------------
# Step 3: Create IAM Role for IRSA
# -----------------------------------------------
echo "🔐 Step 3: Creating IAM role for IRSA..."

# Get OIDC provider
OIDC_PROVIDER=$(aws eks describe-cluster --name ${CLUSTER_NAME} \
  --query "cluster.identity.oidc.issuer" --output text | sed 's|https://||')

# Create trust policy
cat > /tmp/trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::${ACCOUNT_ID}:oidc-provider/${OIDC_PROVIDER}"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "${OIDC_PROVIDER}:sub": "system:serviceaccount:${NAMESPACE}:spark-sa",
          "${OIDC_PROVIDER}:aud": "sts.amazonaws.com"
        }
      }
    }
  ]
}
EOF

aws iam create-role \
  --role-name ${IAM_ROLE} \
  --assume-role-policy-document file:///tmp/trust-policy.json \
  2>/dev/null || echo "   (Role already exists)"

# Attach S3 full access
aws iam attach-role-policy \
  --role-name ${IAM_ROLE} \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

echo "   ✅ IAM role: ${IAM_ROLE} (S3 access via IRSA)"
echo ""

# -----------------------------------------------
# Step 4: Apply Kubernetes RBAC
# -----------------------------------------------
echo "⚙️  Step 4: Applying RBAC..."
kubectl apply -f spark-rbac.yaml

echo "   ✅ Namespace: ${NAMESPACE}, ServiceAccount: spark-sa"
echo ""

# -----------------------------------------------
# Step 5: Build and Push Docker Image
# -----------------------------------------------
echo "🐳 Step 5: Building and pushing Spark image..."

# Login to ECR
aws ecr get-login-password --region ${REGION} | \
  docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

# Build
cd spark-docker
docker build -t ${ECR_REPO}:3.5.0 .
cd ..

# Tag and push
docker tag ${ECR_REPO}:3.5.0 ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO}:3.5.0
docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO}:3.5.0

echo "   ✅ Image pushed: ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO}:3.5.0"
echo ""

# -----------------------------------------------
# Step 6: Verify
# -----------------------------------------------
echo "🔍 Step 6: Verifying setup..."
echo ""
echo "   Cluster:"
kubectl cluster-info
echo ""
echo "   Nodes:"
kubectl get nodes
echo ""
echo "   Spark namespace:"
kubectl get all -n ${NAMESPACE}
echo ""

# -----------------------------------------------
# Done
# -----------------------------------------------
echo "============================================"
echo "  ✅ EKS Setup Complete!"
echo "============================================"
echo ""
echo "  To generate 1000x data:"
echo "    bash submit-datagen.sh 1000"
echo ""
echo "  To monitor:"
echo "    kubectl get pods -n spark -w"
echo "    kubectl logs -f <driver-pod> -n spark"
echo ""
echo "  To scale executors manually:"
echo "    eksctl scale nodegroup --cluster ${CLUSTER_NAME} \\"
echo "      --name spark-executors --nodes 5"
echo ""
echo "  To tear down (STOP COSTS):"
echo "    eksctl delete cluster --name ${CLUSTER_NAME}"
echo ""
echo "  Estimated hourly cost:"
echo "    Control plane: \$0.10/hr"
echo "    Driver (m5.large): \$0.096/hr"
echo "    Executors (3x m5.xlarge Spot): ~\$0.19/hr"
echo "    Total: ~\$0.39/hr"
echo ""
echo "============================================"
