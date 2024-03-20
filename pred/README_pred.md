## Commandline arguments

* deploymentMode
* topoName
* input
* experiRunId (FIT/SYS/TAXI/GRID- number)
* scalingFactor
* outputDir: local directory, which will be filled with log-files
* taskProp : path the .properties-file 
* taskName 


## Commands

After starting the cluster as described in the main-README, execute the commands below. Make sure to change the --outputDir to a local directory

### Example command for SYS-Data
```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --deploymentMode L --topoName IdentityTopology --input ./pred/src/main/resources/datasets/SYS_sample_data_senml.csv --experiRunId SYS-210 --scalingFactor 0.01 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./pred/src/main/resources/configs/all_tasks.properties --taskName bench
```

### Example command for TAXI-Data
```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --deploymentMode L --topoName IdentityTopology --input ./pred/src/main/resources/datasets/TAXI_sample_data_senml.csv --experiRunId TAXI-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./pred/src/main/resources/configs/all_tasks.properties --taskName bench
```

### Example command for FIT-Data
```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --deploymentMode L --topoName IdentityTopology --input ./pred/src/main/resources/datasets/FIT_sample_data_senml.csv --experiRunId FIT-210 --scalingFactor 0.001 --outputDir /home/jona/Documents/Bachelor_thesis/logs --taskProp ./pred/src/main/resources/configs/all_tasks.properties --taskName bench
```