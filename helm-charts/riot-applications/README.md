# Kubernetes setup

>[!WARNING]
> Make sure the `Certificate Manager` is installed as well as the `Flink` operator helm chart, before attempting to install this custom chart.
> All other dependencies are handled via this helm chart.
```bash
helm install riot-applications .
```

# Certificate Manager

Install the certificate manager first:

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.15.0/cert-manager.yaml
```

# Flink

Installing the `Flink` operator and deploying a session cluster:
```bash
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm install  flink-operator flink-operator-repo/flink-kubernetes-operator
# INFO: If the cert-manager installation failed, use --set webhook.create=false
helm install  flink-operator flink-operator-repo/flink-kubernetes-operator --set webhook.create=false
```

```bash
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
kubectl apply -f templates/kafka-topic-pred-model.yaml
kubectl apply -f templates/kafka-topic-pred-publish.yaml
kubectl apply -f templates/kafka-topic-train-publish.yaml
kubectl apply -f templates/kafka-topic-train-source.yaml
```

For `Kafka` external `NodePorts` are configured on port `9093` with the `kafka-cluster.yaml`.

# Port Forwarding
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
--topic senml-cleaned
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
After the cluster is configured, Flink jobs can be deployed through `flink run`.

# Prometheus Stack
Install the Prometheus stack before installing this projects custom helm-chart.

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/kube-prometheus-stack
```

## Login
The login credentials for `Grafana` can be acquired as follows:

```bash
kubectl get secret riot-applications-grafana -o jsonpath="{.data.admin-user}" | base64 --decode ; echo
kubectl get secret riot-applications-grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo
```
