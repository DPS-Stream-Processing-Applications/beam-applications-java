# Build
`../gradlew build` or in the project root: `./gradlew etl:build`

> [!IMPORTANT]
> Make sure that you have generated the `csv` files for the respective dataset
> via [../senMlConverter/](../senMlConverter/README.md)

# Run
> [!WARNING]
> Make sure the kubernetes port is forwarded before you try running this program!

```bash
../gradlew run --args $(pwd)/../data/<output_file>.csv
```

## Test the train application

```bash
../gradlew run --args $(pwd)/test_input_TRAIN_SYS.csv
```

## Test the pred application

```bash
../gradlew run --args $(pwd)/test_input_PRED_TAXI.csv
```
