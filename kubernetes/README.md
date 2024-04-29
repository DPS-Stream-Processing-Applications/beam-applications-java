# Kubernetes setup
Installing the `Flink` operator and deploying a session cluster:

```bash
flink operator version 1.8.0 was added to helm as flink-operator-repo
helm install  flink-operator flink-operator-repo/flink-kubernetes-operator
kubectl apply -f flink-session-cluster-deployment.yaml
```

To make the `Flink` web ui available locally:

```bash
kubectl port-forward service/session-cluster-rest 8081:8081
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

Port forwarding command:
```bash
kubectl port-forward service/kafka-cluster-kafka-external-bootstrap 9093:9093
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
