#!/usr/bin/env bash
set -euo pipefail

# Deploy local Prometheus and Registry into the current kind cluster for E2E tests.
# Prometheus is seeded with container_memory_working_set_bytes metrics containing image labels.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="e2e-infra"

echo "[e2e-infra] Creating namespace $NAMESPACE..."
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# --- Deploy local OCI Registry (distribution/distribution) ---
echo "[e2e-infra] Deploying local registry..."
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/registry.yaml"

# --- Deploy Prometheus with pre-loaded metrics ---
echo "[e2e-infra] Deploying Prometheus with seed data..."
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/prometheus-config.yaml"
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/prometheus.yaml"

# --- Deploy nginx proxy exposing Prometheus under a /prometheus base path ---
echo "[e2e-infra] Deploying Prometheus proxy (root + /prometheus base path)..."
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/prometheus-proxy.yaml"

# --- Deploy Loki for image-pull event discovery ---
echo "[e2e-infra] Deploying Loki..."
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/loki.yaml"

# --- Deploy Alloy to ship real Kubernetes events into Loki ---
echo "[e2e-infra] Deploying Alloy (kubernetes_events -> Loki)..."
kubectl apply -f "$SCRIPT_DIR/alloy.yaml"

# --- Wait for readiness ---
echo "[e2e-infra] Waiting for registry to be ready..."
kubectl -n "$NAMESPACE" wait --for=condition=available deployment/registry --timeout=90s

# --- Configure Kind nodes to reach the in-cluster registry ---
# Kubelet/containerd on Kind nodes can't resolve cluster DNS, so we point them
# at the registry's ClusterIP via containerd mirror config.
REGISTRY_IP=$(kubectl -n "$NAMESPACE" get svc registry -o jsonpath='{.spec.clusterIP}')
REGISTRY_HOST="registry.e2e-infra.svc.cluster.local:5000"
echo "[e2e-infra] Configuring containerd mirror on Kind nodes for $REGISTRY_HOST -> $REGISTRY_IP..."

for node in $(kind get nodes --name drop-dev 2>/dev/null || kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
  docker exec "$node" mkdir -p "/etc/containerd/certs.d/$REGISTRY_HOST"
  cat <<EOF | docker exec -i "$node" tee "/etc/containerd/certs.d/$REGISTRY_HOST/hosts.toml" > /dev/null
[host."http://$REGISTRY_IP:5000"]
  capabilities = ["pull", "resolve"]
  skip_verify = true
EOF
done
echo "[e2e-infra] Containerd mirror configured on all nodes."

echo "[e2e-infra] Waiting for Prometheus to be ready..."
kubectl -n "$NAMESPACE" wait --for=condition=available deployment/prometheus --timeout=90s

echo "[e2e-infra] Waiting for Prometheus proxy to be ready..."
kubectl -n "$NAMESPACE" wait --for=condition=available deployment/prometheus-proxy --timeout=90s

echo "[e2e-infra] Waiting for Loki to be ready..."
# Loki single-binary startup can lag behind registry/prometheus in CI clusters.
kubectl -n "$NAMESPACE" wait --for=condition=available deployment/loki --timeout=300s

echo "[e2e-infra] Waiting for Alloy to be ready..."
kubectl -n "$NAMESPACE" wait --for=condition=available deployment/alloy --timeout=120s

# --- Seed the registry with a few images ---
echo "[e2e-infra] Seeding registry with test images..."
REGISTRY_POD=$(kubectl -n "$NAMESPACE" get pods -l app=registry -o jsonpath='{.items[0].metadata.name}')
REGISTRY_SVC="registry.$NAMESPACE.svc.cluster.local:5000"

# Push images into the in-cluster registry by running a job
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/seed-registry-job.yaml"
kubectl -n "$NAMESPACE" wait --for=condition=complete job/seed-registry --timeout=120s 2>/dev/null || true

# --- Seed Prometheus with metrics via remote write ---
echo "[e2e-infra] Seeding Prometheus with image metrics..."
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/seed-metrics-job.yaml"
kubectl -n "$NAMESPACE" wait --for=condition=complete job/seed-metrics --timeout=60s 2>/dev/null || true

# --- Seed Loki with image-pull events (Alloy-style JSON structure) ---
echo "[e2e-infra] Seeding Loki with image-pull events..."
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/seed-loki-job.yaml"
kubectl -n "$NAMESPACE" wait --for=condition=complete job/seed-loki --timeout=180s

echo "[e2e-infra] Infrastructure ready."
echo "  Prometheus: http://prometheus.$NAMESPACE.svc.cluster.local:9090"
echo "  Prometheus (base path): http://prometheus-proxy.$NAMESPACE.svc.cluster.local/prometheus"
echo "  Loki:       http://loki.$NAMESPACE.svc.cluster.local:3100"
echo "  Registry:   http://registry.$NAMESPACE.svc.cluster.local:5000"
