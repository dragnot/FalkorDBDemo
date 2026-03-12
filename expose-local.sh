#!/usr/bin/env bash
set -euo pipefail

K8S_DIR="${K8S_DIR:-k8s}"
DEFAULT_NAMESPACE=""
if [[ -f "${K8S_DIR}/00-namespace.yaml" ]]; then
  DEFAULT_NAMESPACE="$(awk '/^[[:space:]]*name:[[:space:]]*/ { print $2; exit }' "${K8S_DIR}/00-namespace.yaml" || true)"
fi
NAMESPACE="${NAMESPACE:-${DEFAULT_NAMESPACE:-guyl}}"
KUBECTL="${KUBECTL:-kubectl}"

FLASK_LOCAL_PORT="${FLASK_LOCAL_PORT:-8080}"
FALKOR_BROWSER_LOCAL_PORT="${FALKOR_BROWSER_LOCAL_PORT:-3000}"
FALKOR_REDIS_LOCAL_PORT="${FALKOR_REDIS_LOCAL_PORT:-6379}"

if ! command -v "$KUBECTL" >/dev/null 2>&1; then
  echo "error: kubectl not found (set KUBECTL=... if needed)" >&2
  exit 1
fi

cleanup() {
  # Best-effort cleanup of background port-forward processes.
  if [[ -n "${PF_FALKOR_PID:-}" ]]; then
    kill "$PF_FALKOR_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${PF_FLASK_PID:-}" ]]; then
    kill "$PF_FLASK_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

echo "Port-forwarding services in namespace: ${NAMESPACE}"
echo "- FalkorDB Browser: http://localhost:${FALKOR_BROWSER_LOCAL_PORT}"
echo "- FalkorDB Redis:   localhost:${FALKOR_REDIS_LOCAL_PORT}"
echo "- Flask app:        http://localhost:${FLASK_LOCAL_PORT}"
echo "Press Ctrl+C to stop."

echo "Starting: svc/falkordb ${FALKOR_BROWSER_LOCAL_PORT}:3000 ${FALKOR_REDIS_LOCAL_PORT}:6379"
"$KUBECTL" -n "$NAMESPACE" port-forward svc/falkordb \
  "${FALKOR_BROWSER_LOCAL_PORT}:3000" \
  "${FALKOR_REDIS_LOCAL_PORT}:6379" \
  >/dev/null 2>&1 &
PF_FALKOR_PID=$!

# Small delay so failures show up quickly.
sleep 0.3

echo "Starting: svc/flask-app ${FLASK_LOCAL_PORT}:8080"
"$KUBECTL" -n "$NAMESPACE" port-forward svc/flask-app "${FLASK_LOCAL_PORT}:8080" >/dev/null 2>&1 &
PF_FLASK_PID=$!

wait "$PF_FALKOR_PID" "$PF_FLASK_PID"
