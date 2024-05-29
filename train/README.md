## Commandline arguments


* URL for database
* experiRunId (FIT/SYS/TAXI/GRID- number)


## General process

1. Start kafka server
2. Start mongodb server
3. Find server address of both services
4. Start flink application (will run forever)
5. Start kafkaProducer (will also run forever)


## Setting up MongoDb

I used this tutorial to set it up https://devopscube.com/deploy-mongodb-kubernetes/

```bash
git clone https://github.com/techiescamp/kubernetes-mongodb
````

```bash
kubectl apply -f .
```

I used this command to get the ip-address and port of my db
````bash
minikube service --url mongo-nodeport-svc
````

Or on the kubernetes cluster:
```bash
kubectl get nodes -o \
    jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}'
```
This will read the first nodes `InternalIP` to use in the `databaseUrl` argument.
The following commands are automatically inlining this IP adress.

<!-- And get the IP-address of a the worker node -->
<!-- Example address: mongodb: -->
<!-- `mongodb://adminuser:password123@192.168.49.2:32000/` -->

## Commands

### Example command for SYS-Data
```bash
flink run -m localhost:8081 \
    ./pred/build/PredJob.jar \
    --databaseUrl=mongodb://adminuser:password123@$(
        kubectl get nodes -o \
            jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' \
    ):32000/ \
    --experiRunId=SYS-210
```
$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')

### Example command for TAXI-Data
```bash
flink run -m localhost:8081 \
    ./pred/build/PredJob.jar \
    --databaseUrl=mongodb://adminuser:password123@$(
        kubectl get nodes -o \
            jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' \
    ):32000/ \
    --experiRunId=TAXI-210
```

### Example command for FIT-Data
```bash
flink run -m localhost:8081 \
    ./pred/build/PredJob.jar \
    --databaseUrl=mongodb://adminuser:password123@$(
        kubectl get nodes -o \
            jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' \
    ):32000/ \
    --experiRunId=FIT-210
```
---

<!--
## Setting up Apache Kafka
In the `kafkaProducer` directory is a deployment.yaml file
```bash
kubectl apply -f  deployment.yaml -n kafka 
```
This will deploy the Apache cluster.


Use the following command to get the Kafka-server-bootstrap address
```bash
kubectl get kafka my-cluster -o=jsonpath='{.status.listeners[*].bootstrapServers}{"\n"}' -n kafka
```

This commands tears down the cluster

```bash
kubectl -n kafka delete $(kubectl get strimzi -o name -n kafka)
```

## Setting up Apache Kafka Producer
In the `kafkaProducer` directory is a Dockerfile.

Create the image from Dockerfile:

```bash
minikube image build -t kafka-producer -f ./Dockerfile .
```

**Note**
Please make sure that the bootstrapserver, the application and the expected dataset are correct.
```bash
kubectl run kafka-producer --image=kafka-producer --image-pull-policy=Never --restart=Never --env="BOOTSTRAP_SERVER=192.168.49.2:31316" --env="APPLICATION=train" --env="DATASET=SYS" --env="SCALING=0.001" --env="TOPIC=test-1" 
```

### Example command for the CITY dataset
```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --deploymentMode L --topoName IdentityTopology --input ./train/src/main/resources/datasets/inputFileForTimerSpout-CITY.csv --inputTrainSet ./train/src/main/resources/datasets/SYS_sample_data_senml.csv --experiRunId SYS-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./train/src/main/resources/configs/all_tasks.properties --taskName bench
```

### Example command for the TAXI dataset
```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --deploymentMode L --topoName IdentityTopology --input ./train/src/main/resources/datasets/inputFileForTimerSpout-TAXI.csv  --inputTrainSet ./train/src/main/resources/datasets/TAXI_sample_data_senml.csv --experiRunId TAXI-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./train/src/main/resources/configs/all_tasks.properties --taskName bench
```

```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --deploymentMode L --topoName IdentityTopology --input ./train/src/main/resources/datasets/inputFileForTimerSpout-TAXI.csv  --inputTrainSet /home/jona/Documents/Bachelor_thesis/Datasets/output_TAXI_small.csv --experiRunId TAXI-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./train/src/main/resources/configs/all_tasks.properties --taskName bench
```

### Example command for the FIT dataset
```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --deploymentMode L --topoName IdentityTopology --input ./train/src/main/resources/datasets/inputFileForTimerSpout-FIT.csv  --inputTrainSet ./train/src/main/resources/datasets/FIT_sample_data_senml.csv --experiRunId FIT-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./train/src/main/resources/configs/all_tasks.properties --taskName bench
```

```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --deploymentMode L --topoName IdentityTopology --input ./train/src/main/resources/datasets/inputFileForTimerSpout-FIT.csv  --inputTrainSet /home/jona/Documents/Bachelor_thesis/Datasets/output_FIT_small.csv --experiRunId FIT-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./train/src/main/resources/configs/all_tasks.properties --taskName bench
```
-->

