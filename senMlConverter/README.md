# Build the Docker Image
```bash
docker build -t senml_converter .
```

# Preprocessing
>[!NOTE]
> Please note that the processing can take some time,
> because the application is not performance-optimized at all. 

# Building Datasets

>[!IMPORTANT]
> The docker container needs access to the local file system to read the input files and write the output file.
> This is achieved by mounting the target directory as a [volume])(https://docs.docker.com/storage/volumes/).
> Moving forward, the `data` directory of the project root is the target volume.

## Environment Variables
This application uses environment variables as input for runtime parameters.
The following are used independently of the dataset that is being processed:
| Environment Variable | Description                                                |
|----------------------|------------------------------------------------------------|
| `DATASET`            | Represents the dataset to be processed. Possible values:   |
|                      | `TAXI`, `FIT`, `GRID`.                                     |
| `OUTPUT_FILE`        | Path of the output file. It should be an absolute path     |
|                      | relative to the Docker container's `/home` directory.      |
|                      | Example: `/home/grid_events.csv`.                          |
| `SCALING`            | Scaling factor for the elapsed time calculation.           |
|                      | Timestamps are in UNIX format. Elapsed time is calculated  |
|                      | relative to the first timestamp (`start_time`). Formula:   |
|                      | \[(timestamp - start_time) * scaling_factor\]. This        |
|                      | preserves the original time distribution, impacting only   |
|                      | the frequency of events.                                   |

- `DATASET`: This variable represents the dataset to be processed.
  Possible values are: `TAXI`, `FIT` and `GRID`.
- `OUTPUT_FILE`: This variable represents the path of the output file.
  It should be an absolute path relative to the docker containers `/home` directory.
  An example path could look like this: `/home/grid_events.csv`
- `SCALING`: All datasets provide timestamps of the time they were collected. These are `UNIX` format timestamps.
  From these timestamps, the elapsed time relative to the first timestamp `start_time` is calculated.
  This `relative_elapsed_time` can be scaled through a scaling factor.
  ```math
  (timestamp - start\_time) * scaling\_factor
  ```
  With this implementation the original time distribution is preserved and only the frequency of events is impacted.

## TAXI Dataset
Used for this is the `FOIL2013.zip`, which can be downloaded from [here](https://databank.illinois.edu/datasets/IDB-9610843).
From this zip file the files `trip_data_1.csv` and `trip_fare_1.csv` are required. 

Place these files into the `data` folder of this project.
When running the docker container as shown below, two files will be created in the `data` folder.
`input_joined.csv` contains the joined table of these two datasets, while the `output_taxi.csv` file contains the senml-output format used for the kafkaProducer.
For this creating entries from the 2013-01-14 to the 2013-01-21 will be included, as described in the RIOTBench paper.
Because these seven days would be too long for the benchmark the milliseconds are divided by a scaling factor which can be specified.

```bash
docker run --rm -it \
    -v $PWD/../data:/home \
    -e DATASET="TAXI" \
    -e INPUT_FILE_FARE="/home/trip_fare_1.csv" \
    -e INPUT_FILE_TRIP="/home/trip_data_1.csv" \
    -e OUTPUT_FILE="/home/output_taxi.csv" \
    -e SCALING="260" \
    senml_converter
```

## FIT Dataset
Used for this is the mhealth+dataset.zip, which can be downloaded from this website [FIT download](https://archive.ics.uci.edu/dataset/319/mhealth+dataset). Unzip the file into the `data` folder

```bash
docker run --rm -it -v \
    $PWD/../data:/home \
    -e DATASET="FIT" \
    -e OUTPUT_FILE="/home/output_fit.csv" \
    -e SCALING="260" \
    senml_converter
```

## GRID Dataset
This dataset was provided with the demand of confidentiality therefore, it is not available publicly.
The zip archive will require the [pkware zip tool](https://www.pkware.com/products/zip-reader) to unzip.
It is also `password protected`.

After unzipping, you should get the following folder structure:

    📁 CER Electricity Revised March 2012
    ├── 📁 CER_Electricity_Data
    ├── 📁 CER_Electricity_Documentation
    ├── 📦 File1.txt.zip
    ├── 📦 File2.txt.zip
    ├── 📦 File3.txt.zip
    ├── 📦 File4.txt.zip
    ├── 📦 File5.txt.zip
    └── 📦 File6.txt.zip

The Folders starting with `File` are regular `zip` archives containing `txt` files with the same name.
These are the files you want to move into the `data` in this directory.

>[!NOTE] You can also choose a subset of the files if you are after a smaller sample size.

    📁 data
    ├── 📄 File1.txt
    ├── 📄 File2.txt
    ├── 📄 File3.txt
    ├── 📄 File4.txt
    ├── 📄 File5.txt
    └── 📄 File6.txt

>[!NOTE]
> The `OUTPUT_FILE` argument should use the path relative to the name of the mounted volume.
> In this example this projects `data` directory is mounted as `home`.

```bash
docker run --rm -it -v \
    $PWD/../data:/home \
    -e DATASET="GRID" \
    -e OUTPUT_FILE="/home/output_grid.csv" \
    -e SCALING="0.5" \
    senml_converter
```

