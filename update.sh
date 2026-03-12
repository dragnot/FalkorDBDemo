#!/usr/bin/env bash
set -euo pipefail

K8S_DIR="${K8S_DIR:-k8s}"
DEFAULT_NAMESPACE=""
if [[ -f "${K8S_DIR}/00-namespace.yaml" ]]; then
  DEFAULT_NAMESPACE="$(awk '/^[[:space:]]*name:[[:space:]]*/ { print $2; exit }' "${K8S_DIR}/00-namespace.yaml" || true)"
fi
NAMESPACE="${NAMESPACE:-${DEFAULT_NAMESPACE:-guyl}}"
KUBECTL="${KUBECTL:-kubectl}"
IMAGE="${IMAGE:-}"

if ! command -v "$KUBECTL" >/dev/null 2>&1; then
  echo "error: kubectl not found (set KUBECTL=... if needed)" >&2
  exit 1
fi

"$KUBECTL" apply -f "${K8S_DIR}/00-namespace.yaml"
"$KUBECTL" apply -f "${K8S_DIR}/10-falkordb-pvc.yaml"
"$KUBECTL" apply -f "${K8S_DIR}/11-falkordb-deployment.yaml"
"$KUBECTL" apply -f "${K8S_DIR}/12-falkordb-service.yaml"
"$KUBECTL" apply -f "${K8S_DIR}/21-flask-deployment.yaml"
"$KUBECTL" apply -f "${K8S_DIR}/22-flask-service.yaml"

if [[ -n "$IMAGE" ]]; then
  "$KUBECTL" -n "$NAMESPACE" set image deployment/flask-app "flask=${IMAGE}"
fi

# Force pods to restart so changes/images take effect.
"$KUBECTL" -n "$NAMESPACE" rollout restart deployment/flask-app
"$KUBECTL" -n "$NAMESPACE" rollout status deployment/flask-app

echo "Updated. Namespace: ${NAMESPACE}"
