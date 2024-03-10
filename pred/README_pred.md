## Commandline arguments

* deploymentMode
* topoName
* input
* experiRunId (FIT/SYS/GRID/TAXI- number)
* scalingFactor
* outputDir
* taskProp 
* taskName 





Example command
```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --deploymentMode L --topoName IdentityTopology --input ./pred/src/main/resources/datasets/SYS_sample_data_senml.csv --experiRunId SYS-210 --scalingFactor 0.01 --outputDir /home/jona/Documents/Bachelor_thesis/logs/ --taskProp ./pred/src/main/resources/configs/tasks_CITY.properties --taskName bench
