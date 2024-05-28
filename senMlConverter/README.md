# Build the Docker Image
```bash
docker build -t senml_converter .
```

# Preprocessing
>[!NOTE]
> Please note that the processing can take some time,
> because the application is not performance-optimized at all. 

>[!NOTE]
> The docker container should be run with the `./data` folder of this project.
> This is achieved via `$PWD/../data:/home`.

## TRAIN Dataset

No file needs to be placed inside the `data` folder. Duration is the total time of the benchmark
in minutes, interval is how often the model should train inside the benchmark time. 

```bash
docker run --rm -it -v \
    $PWD/data:/home \
    -e DATASET="TRAIN" \
    -e OUTPUT_FILE="/home/output_train.csv" \
    -e INTERVAL="30" \
    -e DURATION="60" \
    senml_converter
```



## TAXI Dataset
Used for this is the `FOIL2013.zip`, which can be downloaded from [here](https://databank.illinois.edu/datasets/IDB-9610843).
From this zip file the files `trip_data_1.csv` and `trip_fare_1.csv` are required. 

Place these two files into the `data` folder of this project.
When running the docker container as shown below, two files will be created in the `data` folder.
`input_joined.csv` contains the joined table of these two datasets, while the `output_taxi.csv` file contains the senml-output format used for the kafkaProducer.
For this creating entries from the 2013-01-14 to the 2013-01-21 will be included, as described in the RIOTBench paper.
Because these seven days would be too long for the benchmark the milliseconds are divided by a scaling factor which can be specified.

```bash
docker run --rm -it -v \
    $PWD/data:/home \
    -e DATASET="TAXI" \
    -e INPUT_FILE_FARE="/home/trip_fare_1.csv" \
    -e INPUT_FILE_TRIP="/home/trip_data_1.csv" \
    -e OUTPUT_FILE="/home/output_taxi.csv" \
    -e SCALING="260" \
    senml_converter
```

## FIT Dataset
Used for this is the mhealth+dataset.zip, which can be downloaded from this website [FIT download](https://archive.ics.uci.edu/dataset/319/mhealth+dataset). Unzip the folder into the `data` folder. The `mHEALTHDATASET` folder should be the only folder in the `data` folder.

```bash
docker run --rm -it -v \
    $PWD/data:/home \
    -e DATASET="FIT" \
    -e OUTPUT_FILE="/home/output_fit.csv" \
    -e SCALING="660" \
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
    -e SCALING="260" \
    senml_converter
```


## SYS Dataset

This is a workaround, as of now the timestamps from the grid dataset are used and
values based on the example data for SYS in the riotbench repository are used.

Running it like the grid example above:

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


```bash
docker run --rm -it -v $PWD/data:/home -e DATASET="SYS" -e OUTPUT_FILE="/home/output_sys.csv" -e SCALING="160" senml_converter
```

