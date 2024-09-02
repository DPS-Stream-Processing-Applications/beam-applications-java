# StatefunStarter
## General idea
The StatefunStarter starts the statefun-functions, when a record arrives for the functions and shuts it down if there are no incoming records.
The Kafkaproducer sends records to a topic (statefun-starter-input), and these records are processed by the statefun functions. The system includes an automatic shutdown feature that terminates idle services after 60 seconds of inactivity, ensuring efficient resource utilization.

## Setup steps

>[!WARNING]
> Make sure to adapt the values ``Dataset`` and ``MONGODB_ADDRESS`` in the ``statefunStarter-manifest.yaml`` before deploying



```bash
kubectl apply -f service-account.yaml
```

```bash
kubectl apply -f role.yaml
```

```bash
kubectl apply -f rolebinding.yaml
```

```bash
kubectl apply -f kafka-topic-statefun-starter-input.yaml
```

```bash
kubectl apply -f statefunStarter-manifest.yaml -n statefun
```