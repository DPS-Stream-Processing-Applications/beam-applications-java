.PHONY: configmap image service

image:
	export $(shell minikube docker-env)
	docker build . -t scheduler-image

service: 
	kubectl create -f scheduler-manifest.yaml

delete:
	kubectl delete -f scheduler-manifest.yaml

logs:
	kubectl logs -l app=scheduler