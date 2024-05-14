# Build
`../gradlew build` or in the project root: `./gradlew etl:build`

# Run
> [!WARN]
> Make sure the kubernetes port is forwarded before you try running this program!

```bash
../gradlew run --args $(pwd)/test_TAXI.csv
# ../gradlew run --args "/home/eprader/beam-applications-java/nkafkaProducer/test.csv"
```

## Test the train application

```bash
../gradlew run --args $(pwd)/test_input_TRAIN_SYS.csv
```

## Test the pred application

```bash
../gradlew run --args $(pwd)/test_input_PRED_TAXI.csv
```
