# Build
`../gradlew build` or in the project root: `./gradlew etl:build`

> [!IMPORTANT]
> Make sure that you have generated the `csv` files for the respective dataset
> via [../senMlConverter/](../senMlConverter/README.md)

# Run
> [!WARNING]
> Make sure the kubernetes port is forwarded before you try running this program!
Because gradle handles interrupts differently, we launch the producer standalone.

```bash
../gradlew build
java -jar build/KafkaProducer.jar <path_to_csv_file>  <number_of_threads>
```
