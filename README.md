# falkordemo

Small demo showing **Flask** talking to **FalkorDB** (RedisGraph commands) and rendering a simple network visualization in the browser.

## What’s inside

- Flask app: `app/app.py`
  - `GET /` UI (single HTML page using `vis-network` from a CDN)
  - `GET /api/graph` seeds and returns nodes/edges
  - `POST /api/reseed` drops + recreates the demo graph
- Container build: `app/Dockerfile`
- Kubernetes manifests: `k8s/`
  - FalkorDB (PVC + Deployment + Service)
  - Flask app (Deployment + Service)

## Prereqs

- Local run: Docker
- Kubernetes run: `kubectl` + a working cluster

## Run locally (Docker)

### 1) Start FalkorDB

```bash
docker run -d --name falkordb \
  -p 6379:6379 -p 3000:3000 \
  falkordb/falkordb:latest
```

(Optional) FalkorDB Browser should be available at http://localhost:3000

### 2) Build + run the Flask app container

From repo root:

```bash
docker build -t falkordemo-flask:local ./app

docker run --rm -p 8080:8080 \
  -e FALKOR_HOST=host.docker.internal \
  -e FALKOR_PORT=6379 \
  -e GRAPH_NAME=demo \
  falkordemo-flask:local
```

Open the UI:

- http://localhost:8080

## Run on Kubernetes

### 1) Apply manifests

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/10-falkordb-pvc.yaml
kubectl apply -f k8s/11-falkordb-deployment.yaml
kubectl apply -f k8s/12-falkordb-service.yaml
kubectl apply -f k8s/21-flask-deployment.yaml
kubectl apply -f k8s/22-flask-service.yaml
```

### 2) Port-forward the Flask service

```bash
kubectl -n guyl port-forward svc/flask-app 8080:8080
```

Then open:

- http://localhost:8080

### 3) (Optional) Port-forward FalkorDB Browser

```bash
kubectl -n guyl port-forward svc/falkordb 3000:3000
```

Browser UI:

- http://localhost:3000

## Image note (Kubernetes)

The manifest `k8s/21-flask-deployment.yaml` currently references `guyl/falkordemo-flask:0.1`.

If you want to push your own image, build/tag/push and then update the deployment image, e.g.:

```bash
# example only — replace with your Docker Hub / GHCR repo
docker build -t <your-registry>/falkordemo-flask:0.1 ./app
docker push <your-registry>/falkordemo-flask:0.1

kubectl -n guyl set image deployment/flask-app flask=<your-registry>/falkordemo-flask:0.1
```

## Configuration

Environment variables (Flask container):

- `FALKOR_HOST` (default: `falkordb`)
- `FALKOR_PORT` (default: `6379`)
- `GRAPH_NAME` (default: `demo`)
