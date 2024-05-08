# Kubernetes Setup


## Prerequisites


```
minikube config set memory 5120
minikube start
minikube ssh 'sudo ip link set docker0 promisc on'
eval $(minikube -p minikube docker-env)
```

## Create the `statefun` namespace.

```
kubectl create -f 00-namespace
```

## Create auxiliary services that are needed:
 
```
kubectl create -f 01-minio -n statefun
``` 

## Create the function:

### For the train application

```
cd 03-train-functions
make service
cd ..
```

### For the pred application

```
cd 03-pred-functions
make service
cd ..
```

### Deleting a service

```bash
make delete
````

## Start the StateFun runtime

```
kubectl create -f 04-statefun -n statefun
```

## Open the Flink's WEB UI

```
 kubectl port-forward svc/statefun-master-rest 8081:8081 -n statefun
```

Now you can explore Apache Flink's WEB interface:

[http://localhost:8081/#/overview](http://localhost:8081/#/overview)

## Start the nkafkaProducer

Make sure to forward the kafka port via kubernetes

For the pred application
```bash
../gradlew run --args $(pwd)/test.csv
```

## Teardown

```
kubectl delete namespace statefun
```



