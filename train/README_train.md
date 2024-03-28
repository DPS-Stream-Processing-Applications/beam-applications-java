
## Commandline arguments

* L: Deployment Mode: L for local, while C for cluster
* IdentityTopology: Topology Name, no further usage
* src/main/resources/datasets/inputFileForTimerSpout-CITY.csv: the input file for timer, containing time stamp and rows for database access
* SYS-210: name for the job, name should match with the dataset
* 0.001: scaling factor of the time it takes for the timer to go of
* /home/jona/Documents/Bachelor_thesis/logs: local directory, where logs are stored, Caution: There will be a lot of logs generated!
* src/main/resources/configs/all_tasks.properties: the .properties file with all the important attributes for the application
* bench: Name of the application


## Commands 

After starting the cluster as described in the main-README, execute the commands below. Make sure to change the --outputDir to a local directory

### Adapted commands for fat.jar



#### Example command for the CITY dataset
```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --deploymentMode L --topoName IdentityTopology  --experiRunId SYS-210 --scalingFactor 0.001 --taskName bench
```

#### Example command for the FIT dataset
```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --deploymentMode L --topoName IdentityTopology --experiRunId FIT-210 --scalingFactor 0.001 --taskName bench
```

#### Example command for the TAXI dataset
```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --deploymentMode L --topoName IdentityTopology --experiRunId TAXI-210 --scalingFactor 0.001 --taskName bench
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