.PHONY: configmap image service

image:
	export $(shell minikube docker-env)
	docker build . -t train-functions

service: 
	kubectl create -n statefun -f functions-service.yaml

upgrade:
	kubectl rollout restart deployment/functions -n statefun

logs:
	kubectl logs -f -l component=functions -n statefun

pods:
	kubectl get pods -n statefun -l component=functions

delete:
	kubectl delete deployment functions -n statefun
	kubectl delete svc functions -n statefun

