# README for  the pred-application

## Commandline arguments

* deploymentMode
* topoName
* input
* experiRunId (FIT/SYS/TAXI/GRID- number)
* scalingFactor
* outputDir: local directory, which will be filled with log-files
* taskProp : path the .properties-file 
* taskName 



## General process

1. Start kafka server
2. Start mongodb server
3. Find server address of both services
4. Start flink application
5. Start kafkaProducer


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

Example address: mongodb:
`mongodb://adminuser:password123@192.168.49.2:32000/`



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

## Commands

After starting the cluster as described in the main-README, execute the commands below. Make sure to change the --outputDir to a local directory

### Example command for SYS-Data
```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --databaseUrl mongodb://adminuser:password123@x:32000/ --topoName IdentityTopology --experiRunId SYS-210  --taskName bench  --bootstrap x.x --topic test-1
```

### Example command for TAXI-Data
```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --databaseUrl mongodb://adminuser:password123@x:32000/ --topoName IdentityTopology --experiRunId TAXI-210  --taskName bench  --bootstrap x.x --topic test-1
```

### Example command for FIT-Data
```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --databaseUrl mongodb://adminuser:password123@x:32000/ --topoName IdentityTopology --experiRunId FIT-210  --taskName bench  --bootstrap x.x --topic test-1
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
kubectl run kafka-producer --image=kafka-producer --image-pull-policy=Never --restart=Never --env="BOOTSTRAP_SERVER=192.168.49.2:31316" --env="APPLICATION=pred" --env="DATASET=SYS" --env="SCALING=0.001" --env="TOPIC=test-1" 
```


<!--
```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --deploymentMode L --topoName IdentityTopology --input ./pred/src/main/resources/datasets/SYS_sample_data_senml.csv --experiRunId SYS-210 --scalingFactor 0.01 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./pred/src/main/resources/configs/all_tasks.properties --taskName bench
```

### Example command for TAXI-Data
```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --deploymentMode L --topoName IdentityTopology --input ./pred/src/main/resources/datasets/TAXI_sample_data_senml.csv --experiRunId TAXI-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./pred/src/main/resources/configs/all_tasks.properties --taskName bench
```


```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --deploymentMode L --topoName IdentityTopology --input /home/jona/Documents/Bachelor_thesis/Datasets/output_TAXI_small.csv --experiRunId TAXI-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./pred/src/main/resources/configs/all_tasks.properties --taskName bench
```



Command for my local cluster
/opt/flink/flink-1.18.1/bin/flink run -m localhost:8081 ./pred/build/PredJob.jar --deploymentMode L --topoName IdentityTopology --input /home/jona/Documents/Bachelor_thesis/Datasets/output_TAXI_small.csv --experiRunId TAXI-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./pred/src/main/resources/configs/all_tasks_local.properties --taskName bench


### Example command for FIT-Data
```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --deploymentMode L --topoName IdentityTopology --input ./pred/src/main/resources/datasets/FIT_sample_data_senml.csv --experiRunId FIT-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./pred/src/main/resources/configs/all_tasks.properties --taskName bench
```


```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --deploymentMode L --topoName IdentityTopology --input /home/jona/Documents/Bachelor_thesis/Datasets/output_FIT.csv --experiRunId FIT-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./pred/src/main/resources/configs/all_tasks.properties --taskName bench
```
-->
