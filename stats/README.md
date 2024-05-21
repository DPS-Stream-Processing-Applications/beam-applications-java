The default build command will build this Job for the `TAXI` dataset.

```bash
../gradlew build
flink run ./build/FlinkJob.jar
```

If you want to compile using a different main class you can do so by overriding `mainClass` with the `-P` flag.

Example for the `FIT` implementation:
```bash
../gradlew build -PmainClass=at.ac.uibk.dps.streamprocessingapplications.etl.FlinkJobFIT
flink run ./build/FlinkJob.jar
```
