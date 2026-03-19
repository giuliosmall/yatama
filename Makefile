# --------------------------------------------------------------------------- #
#  Task Manager -- Local Kubernetes Development Makefile                       #
# --------------------------------------------------------------------------- #

CLUSTER_NAME  := task-manager
IMAGE_NAME    := task-manager
IMAGE_TAG     := latest
DEPLOY_DIR    := deploy

# Helm release name for PostgreSQL. The Bitnami chart creates a service called
# <release>-postgresql, so the configmap DATABASE_URL must match.
PG_RELEASE    := postgres

.PHONY: all clean build deploy deploy-pg wait-pg deploy-app deploy-prometheus deploy-grafana kind-up kind-down migrate \
	deploy-strimzi deploy-kafka deploy-pgbouncer deploy-api deploy-worker deploy-all-scaled

# ---- Aggregate targets ---------------------------------------------------- #

all: kind-up build deploy
	@echo "--- cluster is ready; app is accessible at http://localhost:8080 ---"

clean: kind-down
	@echo "--- cluster deleted ---"

# ---- Kind cluster --------------------------------------------------------- #

kind-up:
	@if kind get clusters 2>/dev/null | grep -q "^$(CLUSTER_NAME)$$"; then \
		echo "kind cluster '$(CLUSTER_NAME)' already exists"; \
	else \
		kind create cluster --name $(CLUSTER_NAME) --config $(DEPLOY_DIR)/kind-config.yaml; \
	fi

kind-down:
	kind delete cluster --name $(CLUSTER_NAME)

# ---- Docker build + load into Kind --------------------------------------- #

build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
	kind load docker-image $(IMAGE_NAME):$(IMAGE_TAG) --name $(CLUSTER_NAME)

# ---- Deploy (Postgres + migrations + app) --------------------------------- #

deploy: deploy-pg wait-pg migrate deploy-app deploy-prometheus deploy-grafana

deploy-pg:
	helm install $(PG_RELEASE) oci://registry-1.docker.io/bitnamicharts/postgresql \
		--values $(DEPLOY_DIR)/postgres-values.yaml \
		--wait --timeout 120s

wait-pg:
	@echo "waiting for PostgreSQL pod to be ready..."
	kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=postgresql --timeout=120s

migrate:
	@echo "applying migrations..."
	kubectl exec -i $$(kubectl get pod -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}') \
		-- env PGPASSWORD=taskmanager psql -U postgres -d taskmanager < migrations/001_create_tables.up.sql
	kubectl exec -i $$(kubectl get pod -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}') \
		-- env PGPASSWORD=taskmanager psql -U postgres -d taskmanager < migrations/002_create_dead_letter_queue.up.sql
	kubectl exec -i $$(kubectl get pod -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}') \
		-- env PGPASSWORD=taskmanager psql -U postgres -d taskmanager < migrations/003_add_run_after.up.sql
	kubectl exec -i $$(kubectl get pod -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}') \
		-- env PGPASSWORD=taskmanager psql -U postgres -d taskmanager < migrations/004_add_reaper_and_idempotency.up.sql
	kubectl exec -i $$(kubectl get pod -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}') \
		-- env PGPASSWORD=taskmanager psql -U postgres -d taskmanager < migrations/005_optimize_for_scale.up.sql

deploy-app:
	kubectl apply -f $(DEPLOY_DIR)/configmap.yaml
	kubectl apply -f $(DEPLOY_DIR)/deployment.yaml
	kubectl apply -f $(DEPLOY_DIR)/service.yaml
	kubectl rollout status deployment/task-manager --timeout=60s

deploy-prometheus:
	kubectl apply -f $(DEPLOY_DIR)/prometheus-config.yaml
	kubectl apply -f $(DEPLOY_DIR)/prometheus.yaml
	kubectl rollout status deployment/prometheus --timeout=60s

deploy-grafana:
	kubectl apply -f $(DEPLOY_DIR)/grafana-datasource.yaml
	kubectl apply -f $(DEPLOY_DIR)/grafana-dashboard.yaml
	kubectl apply -f $(DEPLOY_DIR)/grafana.yaml
	kubectl rollout status deployment/grafana --timeout=60s

# ---- Scaled deployment (Kafka + PgBouncer + separate API/Worker) ---------- #

# Image names for the split binaries.
API_IMAGE     := task-manager-api
WORKER_IMAGE  := task-manager-worker

# Strimzi operator version.
STRIMZI_VERSION := 0.40.0

deploy-strimzi:
	@echo "installing Strimzi Kafka operator $(STRIMZI_VERSION)..."
	kubectl create -f https://strimzi.io/install/latest?namespace=default 2>/dev/null || \
		echo "Strimzi operator already installed"
	@echo "waiting for Strimzi operator to be ready..."
	kubectl wait --for=condition=ready pod -l name=strimzi-cluster-operator --timeout=120s

deploy-kafka: deploy-strimzi
	@echo "deploying Kafka cluster and topics..."
	kubectl apply -f $(DEPLOY_DIR)/kafka/strimzi-kafka.yaml
	@echo "waiting for Kafka cluster to be ready (this may take several minutes)..."
	kubectl wait kafka/task-manager-kafka --for=condition=Ready --timeout=300s
	kubectl apply -f $(DEPLOY_DIR)/kafka/topics.yaml

deploy-pgbouncer:
	@echo "deploying PgBouncer..."
	kubectl apply -f $(DEPLOY_DIR)/pgbouncer.yaml
	kubectl rollout status deployment/pgbouncer --timeout=60s

deploy-api:
	@echo "building API image..."
	docker build -t $(API_IMAGE):$(IMAGE_TAG) -f Dockerfile.api .
	kind load docker-image $(API_IMAGE):$(IMAGE_TAG) --name $(CLUSTER_NAME)
	kubectl apply -f $(DEPLOY_DIR)/configmap-api.yaml
	kubectl apply -f $(DEPLOY_DIR)/api-deployment.yaml
	kubectl apply -f $(DEPLOY_DIR)/api-service.yaml
	kubectl apply -f $(DEPLOY_DIR)/api-hpa.yaml
	kubectl rollout status deployment/task-manager-api --timeout=120s

deploy-worker:
	@echo "building worker image..."
	docker build -t $(WORKER_IMAGE):$(IMAGE_TAG) -f Dockerfile.worker .
	kind load docker-image $(WORKER_IMAGE):$(IMAGE_TAG) --name $(CLUSTER_NAME)
	kubectl apply -f $(DEPLOY_DIR)/configmap-worker.yaml
	kubectl apply -f $(DEPLOY_DIR)/worker-deployment.yaml
	kubectl apply -f $(DEPLOY_DIR)/worker-hpa.yaml 2>/dev/null || echo "KEDA not installed — skipping worker HPA (install KEDA for Kafka-lag autoscaling)"
	kubectl rollout status deployment/task-manager-worker --timeout=120s

deploy-all-scaled: kind-up deploy-pg wait-pg migrate deploy-kafka deploy-pgbouncer deploy-api deploy-worker deploy-prometheus deploy-grafana
	@echo "--- scaled cluster is ready ---"
	@echo "  API:        http://localhost:8080 (via port-forward or ingress)"
	@echo "  Prometheus: http://localhost:3000 (Grafana)"
