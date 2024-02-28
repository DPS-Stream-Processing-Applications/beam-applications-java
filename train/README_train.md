
## Commandline arguments

* L: Deployment Mode
* IdentityTopology: Topology Name
* src/main/resources/datasets/inputFileForTimerSpout-CITY.csv
* SYS-210
* 0.001
* /home/jona
* src/main/resources/configs/tasks_CITY.properties
* jona

Example command
```bash
flink run -m localhost:8081 ./train/build/TrainJob.jar --deploymentMode L --topoName IdentityTopology --input ./train/src/main/resources/datasets/inputFileForTimerSpout-CITY.csv --experiRunId SYS-210 --scalingFactor 0.001 --outputDir /home/jona --taskProp ./train/src/main/resources/configs/tasks_CITY.properties --taskName jona
