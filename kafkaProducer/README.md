# Build
`../gradlew build` or in the project root: `./gradlew etl:build`

# Run
> [!WARN]
> Make sure the kubernetes port is forwarded before you try running this program!

```bash
../gradlew run --args $(pwd)/test.csv
# ../gradlew run --args "/home/eprader/beam-applications-java/nkafkaProducer/test.csv"
```
