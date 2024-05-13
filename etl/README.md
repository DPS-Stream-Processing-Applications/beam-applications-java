# Build
`../gradlew build` or in the project root: `./gradlew etl:build`

# Run

With local cluster (`start-cluster.sh`) or make sure you have port 8081 forwarded from the Kubernetes cluster: `flink run -m localhost:8081 ./build/FlinkJob.jar` or `flink run -m localhost:8081 ./etl/build/FlinkJob.jar`