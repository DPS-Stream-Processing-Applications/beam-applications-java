## Commandline arguments

* deploymentMode
* topoName
* input
* experiRunId (FIT/SYS/TAXI/GRID- number)
* scalingFactor
* outputDir
* taskProp : path the .properties-file 
* taskName 


Example command for Sys-DataSet
```bash
flink run -m localhost:8081 ./pred/build/PredJob.jar --deploymentMode L --topoName IdentityTopology --input ./pred/src/main/resources/datasets/SYS_sample_data_senml.csv --experiRunId SYS-210 --scalingFactor 0.01 --outputDir /home/jona/Documents/Bachelor_thesis/logs/ --taskProp ./pred/src/main/resources/configs/all_tasks.properties --taskName bench
