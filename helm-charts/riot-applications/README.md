# Kubernetes setup
>[!WARNING]
> Make sure the operators for `Flink` and `Kafka` as well as the `Prometheus Stack` are installed via `helm` before attempting to install this custom chart. 
> Then, this chart can be installed using the following command: `helm install riot-applications .`

# Flink

Install the certificate manager first:
```bash
kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
```
Installing the `Flink` operator and deploying a session cluster:
```bash
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm install  flink-operator flink-operator-repo/flink-kubernetes-operator

kubectl apply -f templates/flink-session-cluster-deployment.yaml
```
# Kafka
Installing the kafka operator as well as setting up the topics:
```bash
helm repo add strimzi https://strimzi.io/charts/
helm install kafka-operator strimzi/strimzi-kafka-operator

kubectl apply -f templates/kafka-cluster.yaml 
kubectl apply -f templates/kafka-topic-senml-source.yaml 
kubectl apply -f templates/kafka-topic-senml-cleaned.yaml 
```

For `Kafka` external `NodePorts` are configured on port `9093` with the `kafka-cluster.yaml`.

# Portforwarding
To forward all ports run the following bash script.

```bash
./utils/portforwarding.sh
```

## Testing Kafka

### Interactive Producer

```bash
kubectl run kafka-producer -it \
--image=strimzi/kafka:latest-kafka-2.4.0 \
--rm=true --restart=Never \
-- bin/kafka-console-producer.sh \
--broker-list kafka-cluster-kafka-bootstrap:9092 \
--topic senml-source
```

> [!TIP]
> The `nKafkaProducer` module contains a producer which automatically pushes events to `localhost:9093`
> and topic `senml-source`.

### Consumer
```bash
kubectl run kafka-consumer -it \
--image=strimzi/kafka:latest-kafka-2.4.0 \
--rm=true --restart=Never \
-- bin/kafka-console-consumer.sh \
--bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
--topic plots-strings \
```
# Prometheus Stack
Install the prometheus stack before installing this projects custom helm-chart.

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/kube-prometheus-stack
```

## Login
The login credentials for `Grafana` can be acquired as follows:

```bash
kubectl get secret prometheus-grafana -o jsonpath="{.data.admin-user}" | base64 --decode ; echo
kubectl get secret prometheus-grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo
```

# Installing MongoDB

```bash
kubectl apply -f templates/mongodb-deployment.yaml
kubectl apply -f templates/mongodb-client.yaml
kubectl apply -f templates/mongodb-nodeport.yaml
kubectl apply -f templates/mongodb-pv.yaml
kubectl apply -f templates/mongodb-pvc.yaml
kubectl apply -f templates/mongodb-secret.yaml
```
After the cluster is configured Flink jobs can be deployed normally through `flink run <path_to_jar>`.
