# Build

This combined pipeline was implemented for 2 Datasets, `TAXI` and `FIT`
## TAXI

```bash
./gradlew riotbenchsinglejob:build -Ptarget=TAXI

```

## FIT

```bash
./gradlew riotbenchsinglejob:build -Ptarget=FIT
```

# Run 
The pipeline requires the database URL to be passed as a CLI argument. Use the following command to read the URL from the Kubernetes deployment.
```bash
flink run ./build/FlinkJob.jar
```
