

```bash
docker run --rm -v $PWD/data:/home -e INPUT_FILE="/home/output_TAXI_small.csv" -e OUTPUT_FILE="/home/test_output.csv" senml_converter
```