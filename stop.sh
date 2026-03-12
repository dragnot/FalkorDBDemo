#!/usr/bin/env bash
set -euo pipefail

K8S_DIR="${K8S_DIR:-k8s}"
DEFAULT_NAMESPACE=""
if [[ -f "${K8S_DIR}/00-namespace.yaml" ]]; then
  DEFAULT_NAMESPACE="$(awk '/^[[:space:]]*name:[[:space:]]*/ { print $2; exit }' "${K8S_DIR}/00-namespace.yaml" || true)"
fi
NAMESPACE="${NAMESPACE:-${DEFAULT_NAMESPACE:-guyl}}"
KUBECTL="${KUBECTL:-kubectl}"

if ! command -v "$KUBECTL" >/dev/null 2>&1; then
  echo "error: kubectl not found (set KUBECTL=... if needed)" >&2
  exit 1
fi

files=(
  "${K8S_DIR}/22-flask-service.yaml"
  "${K8S_DIR}/21-flask-deployment.yaml"
  "${K8S_DIR}/12-falkordb-service.yaml"
  "${K8S_DIR}/11-falkordb-deployment.yaml"
  "${K8S_DIR}/10-falkordb-pvc.yaml"
  "${K8S_DIR}/00-namespace.yaml"
)

for f in "${files[@]}"; do
  # If the namespace is already terminating/deleted, the rest may fail; ignore.
  "$KUBECTL" delete -f "$f" --ignore-not-found || true
done

echo "Deleted manifests (including PVC + namespace). Namespace: ${NAMESPACE}"
