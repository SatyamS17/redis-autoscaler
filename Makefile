.PHONY: all install monitoring keda operator redis deploy test clean

all: install monitoring keda operator redis

install:
	helm repo add kedacore https://kedacore.github.io/charts || true
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts || true
	helm repo add ot-container-kit https://ot-container-kit.github.io/helm-charts || true
	helm repo update

monitoring:
	helm install monitoring prometheus-community/kube-prometheus-stack --namespace monitoring --create-namespace
	@echo "Waiting for Prometheus to be ready..."
	kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=prometheus -n monitoring --timeout=300s || true

keda:
	helm install keda kedacore/keda --namespace keda --create-namespace
	@echo "Waiting for KEDA to be ready..."
	kubectl wait --for=condition=ready pod -l app=keda-operator -n keda --timeout=300s
	@echo "Waiting for KEDA CRDs to be established..."
	kubectl wait --for=condition=established crd/scaledobjects.keda.sh --timeout=60s

operator:
	helm install redis-operator ot-container-kit/redis-operator --namespace redis-operator --create-namespace -f manifests/redis-operator-helm-values.yaml
	@echo "Waiting for Redis operator to be ready..."
	kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=redis-operator -n redis-operator --timeout=300s || true

redis:
	kubectl apply -f manifests/rediscluster-cr.yaml
	@echo "Waiting for Redis cluster pods..."
	sleep 10
	kubectl apply -f manifests/redis-exporter-servicemonitor.yaml
	kubectl apply -f manifests/prometheus-rules.yaml -n monitoring
	kubectl apply -f manifests/keda-scaledobject.yaml

test:
	kubectl apply -f scripts/load-test.yaml

clean:
	helm uninstall monitoring -n monitoring || true
	helm uninstall keda -n keda || true
	helm uninstall redis-operator -n redis-operator || true
	kubectl delete rediscluster --all || true
	kubectl delete ns monitoring keda redis-operator || true