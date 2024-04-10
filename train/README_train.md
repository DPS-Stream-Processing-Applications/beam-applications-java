



## Commands 

After starting the cluster as described in the main-README, execute the commands below. Make sure to change the --outputDir to a local directory




#### Example command for the CITY dataset
```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --databaseUrl mongodb://adminuser:password123@x:32000/ --topoName IdentityTopology --experiRunId SYS-210  --taskName bench  --bootstrap x.x --topic test-1


```

#### Example command for the FIT dataset
```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --databaseUrl mongodb://adminuser:password123@x:32000/ --topoName IdentityTopology --experiRunId FIT-210  --taskName bench  --bootstrap x.x --topic test-1


```

#### Example command for the TAXI dataset
```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --databaseUrl mongodb://adminuser:password123@x:32000/ --topoName IdentityTopology --experiRunId TAXI-210  --taskName bench  --bootstrap x.x --topic test-1


```


<!--
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