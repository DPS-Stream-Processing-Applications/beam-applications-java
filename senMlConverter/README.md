# Build the Docker Image
Before you are able to run this application make sure to build the docker image.

```bash
docker build -t senml_converter .
```

# Processing Datasets

>[!CAUTION]
> This application makes use of pandas data frames and is not performance optimized.
> Expect a significant runtime for these datasets.

>[!IMPORTANT]
> The docker container needs access to the local file system to read the input files and write the output file.
> This is achieved by mounting the target directory as a [volume])(https://docs.docker.com/storage/volumes/).
> Moving forward, the `data` directory of the project root is the target volume.

## Environment Variables
This application uses environment variables as input for runtime parameters.
The following are used independently of the dataset that is being processed:

| Environment Variable | Description |
|----------------------|-------------|
| `DATASET`            | Represents the dataset to be processed. Possible values: `TAXI`, `FIT`, `GRID`. |
| `OUTPUT_FILE`        | Path of the output file. It should be an absolute path relative to the Docker container's `/home` directory. Example: `/home/grid_events.csv`. |
| `SCALING`            | The Scaling factor for the elapsed time between events. The original timestamps of the datasets are in UNIX format. The elapsed time is calculated relative to the first timestamp (`startTime`). $$(timestamp - startTime) * scalingFactor$$ This preserves the original time distribution, impacting only the frequency of events. |

## TAXI Dataset
Used for this is the `FOIL2013.zip`, which can be downloaded from [this databank](https://databank.illinois.edu/datasets/IDB-9610843).
From this zip file the files `trip_data_*.csv` and `trip_fare_*.csv` are required. 

Place these files into the `data` folder of this project.
When running the docker container as shown below, two files will be created:
- `input_joined.csv` contains the joined table of these two datasets
-  `OUTPUT_FILE` file contains the SenML output format used for the `kafkaProducer`.
   Using the files `trip_fare_1.csv` and `trip_data_1.csv` will result in the same dataset used in the original RIOTBench paper.
   This will contain data from `2013-01-14` to `2013-01-21`.

```bash
docker run --rm -it \
    -v $PWD/../data:/home \
    -e DATASET="TAXI" \
    -e OUTPUT_FILE="/home/riot_events_TAXI.csv" \
    -e SCALING="0.5" \
    senml_converter
```

## FIT Dataset
The dataset used here is [MHEALTH](https://archive.ics.uci.edu/dataset/319/mhealth+dataset), which is available for download from the
UC Irvine Machine Learning Repository.

```bash
docker run --rm -it \
    -v $PWD/../data:/home \
    -e DATASET="FIT" \
    -e OUTPUT_FILE="/home/riot_events_FIT.csv" \
    -e SCALING="0.5" \
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
These are the files you want to move into the `data` directory.

>[!NOTE]
> You can also choose a subset of the files if you are after a smaller sample size.

    📁 data
    ├── 📄 File1.txt
    ├── 📄 File2.txt
    ├── 📄 File3.txt
    ├── 📄 File4.txt
    ├── 📄 File5.txt
    └── 📄 File6.txt

```bash
docker run --rm -it \
    -v $PWD/../data:/home \
    -e DATASET="GRID" \
    -e OUTPUT_FILE="/home/riot_events_GRID.csv" \
    -e SCALING="0.5" \
    senml_converter
```

