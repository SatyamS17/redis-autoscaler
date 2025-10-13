.PHONY: all install monitoring redis deploy test clean

all: install monitoring redis

install:
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts || true
	helm repo update

monitoring:
	helm upgrade --install monitoring prometheus-community/kube-prometheus-stack --namespace monitoring --create-namespace
redis:
	kubectl apply -f manifests/prometheus-rules.yaml -n monitoring

clean:
	helm uninstall monitoring -n monitoring || true
	kubectl delete ns monitoring || true