# Table to SenMl-Converter for the Event-Generator


## Build the docker image

```bash
docker build -t senml_converter .
```


## Preprocessing the taxi dataset
Used for this is the FOIL2013.zip, which can be downloaded from here: [Download taxi data](https://databank.illinois.edu/datasets/IDB-9610843). From this zip file the files `trip_data_1.csv` and `trip_fare_1.csv` are needed. 
Place these files into the `data-folder`. When running the docker container as shown below two files will be created in the `data-folder`. `input_joined.csv` contains the joined table of these two datasets, while the second file contains the senml-output format. For this creating entries from the 2013-01-14 to the 2013-01-21 will be included, as described in the paper. 


```bash
docker run --rm -it -v $PWD/data:/home -e DATASET="TAXI" -e INPUT_FILE_FARE="/home/trip_fare_1.csv" -e INPUT_FILE_TRIP="home/trip_data_1.csv" -e OUTPUT_FILE="/home/output_taxi.csv" senml_converter
```

## Preprocessing the fit dataset
Used for this is the mhealth+dataset.zip, which can be downloaded from this website [FIT download](https://archive.ics.uci.edu/dataset/319/mhealth+dataset). Unzip the file into the `data` folder

```bash
docker run --rm -it -v $PWD/data:/home -e DATASET="FIT" -e OUTPUT_FILE="/home/output_fit.csv"  senml_converter
```

