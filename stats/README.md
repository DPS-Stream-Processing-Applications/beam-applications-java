# Build
The default build of this job is a monolithic job `STATS` which can be configured via the `--experiRunId` command line option later.

```bash
../gradlew build
```

You can also build the job for the specific dataset. Far this the -P

If you want to build the dedicated jobs for each dataset, you can do so by overriding `mainClass` with the `-PmainClass` option.
Available main classes are:

## TAXI

```bash
../gradlew build -PmainClass=at.ac.uibk.dps.streamprocessingapplications.stats.FlinkJobTAXI

```

## FIT

```bash
../gradlew build -PmainClass=at.ac.uibk.dps.streamprocessingapplications.stats.FlinkJobFIT
```

## GRID

```bash
../gradlew build -PmainClass=at.ac.uibk.dps.streamprocessingapplications.stats.FlinkJobGRID
```

# Run

If you chose the default build you can now specify the dataset via the `--experiRunId` option.
Available options: `TAXI`, `FIT` and `GRID`.

Example with `TAXI`.
```bash
flink run ./build/FlinkJob.jar --experiRunId=TAXI
```

For the specific builds you can just omit the `experiRunId` option.

```bash
flink run ./build/FlinkJob.jar
```
