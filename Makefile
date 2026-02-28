# Build images for service
build_kafkainit:
	docker build --no-cache -t local/kafka-init -f src/kafka_init/Dockerfile src/kafka_init/

build_orderbook:
	docker build --no-cache -t local/orderbook -f src/orderbook/Dockerfile src/orderbook/

build_traderpool:
	docker build --no-cache -t local/traderpool -f src/traderpool/Dockerfile src/traderpool/

build: build_kafkainit build_orderbook build_traderpool

helm:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update

clear_helm:
	helm repo remove bitnami

# Local infra
start_infra:
	kubectl apply -f k8s/namespaces.yaml

start_deps:
	helm upgrade --install bitnami bitnami/kafka --version 31.0.0 -n orderbook --create-namespace -f helm/kafka/values-local.yaml

stop_deps:
	helm uninstall --ignore-not-found bitnami -n orderbook
	kubectl delete --ignore-not-found pvc data-bitnami-kafka-controller-0 -n orderbook

start: start_infra start_deps
	helm install orderbook ./chart --namespace orderbook

stop: stop_deps
	helm uninstall --ignore-not-found orderbook --namespace orderbook

# Need virtual env or python setup with requirements installed
# 1. test contracts
# 2. test orderbook
# 3. test traderbook
test:
	cd src/orderbook/ && go test

ruff:
	uv run ruff format . && uv run ruff check --fix