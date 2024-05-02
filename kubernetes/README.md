# Kubernetes setup
Installing the `Flink` operator and deploying a session cluster:

```bash
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm install  flink-operator flink-operator-repo/flink-kubernetes-operator
kubectl apply -f flink-session-cluster-deployment.yaml
```

Installing the kafka operator as well as setting up the topics:
```bash
helm repo add strimzi https://strimzi.io/charts/
helm install kafka-operator strimzi/strimzi-kafka-operator
kubectl apply -f kafka-cluster.yaml 
kubectl apply -f kafka-topic-senml-source.yaml 
kubectl apply -f kafka-topic-senml-cleaned.yaml 
kubectl apply -f kafka-topic-plots-strings.yaml 
```

For `Kafka` external `NodePorts` are configured on port `9093` with the `kafka-cluster.yaml`.

# Portforwarding
To forward all ports run the following bash script.

```bash
./portforwarding.sh
```

# Testing Kafka

## Interactive Producer

```bash
kubectl run kafka-producer -it \
--image=strimzi/kafka:latest-kafka-2.4.0 \
--rm=true --restart=Never \
-- bin/kafka-console-producer.sh \
--broker-list kafka-cluster-kafka-bootstrap:9092 \
--topic senml-source
```

> [!INFO]
> The `nKafkaProducer` module contains a producer which automatically pushes events to `localhost:9093`
> and topic `senml-source`.

## Consumer
```bash
kubectl run kafka-consumer -it \
--image=strimzi/kafka:latest-kafka-2.4.0 \
--rm=true --restart=Never \
-- bin/kafka-console-consumer.sh \
--bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
--topic plots-strings \
```


# Installing MongoDB

```bash
helm repo add mongodb https://mongodb.github.io/helm-charts/
helm install mongodb-operator-crds mongodb/community-operator-crds
helm install mongodb-operator mongodb/community-operator
kubectl apply -f mongodb-deployment.yaml
```

After the cluster is configured Flink jobs can be deployed normally through `flink run -m localhost:8081`.
