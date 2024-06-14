import os
import sys
from typing import List

import pandas as pd
from fit_converter import FitConverter
from fit_modifier import create_table_fit
from grid_converter import GridConverter
from sys_converter import SysConverter
from taxi_converter import TaxiConverter
from taxi_modifier import (
    change_header_order_df,
    drop_cols_fare,
    drop_cols_trip,
    filter_date,
    merge_dfs,
    rename_cols,
)
from train_converter import TrainConverter


def create_table_taxi(input_fare, input_trip):
    output_file = "/home/input_joined.csv"

    filtered_trip = filter_date(input_trip)
    filtered_fare = filter_date(input_fare)
    order = [
        "taxi_identifier",
        " hack_license",
        " pickup_datetime",
        " trip_time_in_secs",
        " trip_distance",
        " pickup_longitude",
        " pickup_latitude",
        " dropoff_longitude",
        " dropoff_latitude",
        " payment_type",
        " fare_amount",
        " surcharge",
        " mta_tax",
        " tip_amount",
        " tolls_amount",
        " total_amount",
    ]

    renamed_trip = rename_cols(filtered_trip)
    renamed_fare = rename_cols(filtered_fare)

    cleaned_trip = drop_cols_trip(renamed_trip)
    cleaned_fare = drop_cols_fare(renamed_fare)

    print("merging trip and fare")
    merged_df = merge_dfs(cleaned_trip, cleaned_fare)
    ordered_df = change_header_order_df(merged_df, order)
    ordered_df.to_csv(output_file, index=False)
    print("stored intermediate joined file")


def convert_fit(input, output, use_riotbench_format, scaling):
    converter = FitConverter(input, output, scaling)
    if use_riotbench_format:
        converter.converter_to_senml_riotbench_csv(10000)
    else:
        converter.convert_to_senml_csv(10000)


def convert_train(output_file, interval, time_bench):
    converter = TrainConverter(output_file, interval, time_bench)
    converter.convert_to_senml_csv(10000)


if __name__ == "__main__":
    dataset: str | None = os.environ.get("DATASET")
    output_file: str | None = os.environ.get("OUTPUT_FILE")
    scaling: str | None = os.environ.get("SCALING")
    scaling_factor: float = float(scaling) if scaling else 1.0

    if dataset == None:
        raise ValueError("Missing required environment variable DATASET")
    if output_file == None:
        raise ValueError("Missing required environment variable OUTPUT_FILE")

    # NOTE: The User mounts `/home` as the volume where the data is stored.
    DATA_DIR_PATH: str = "/home"
    CHUNK_SIZE: int = 10000

    if dataset == "TAXI":
        trip_data_paths: List[str] = [
            f"{DATA_DIR_PATH}/trip_data_{i}.csv"
            for i in range(1, 2)
            if os.path.exists(f"{DATA_DIR_PATH}/trip_data_{i}.csv")
        ]
        trip_fare_paths: List[str] = [
            f"{DATA_DIR_PATH}/trip_fare_{i}.csv"
            for i in range(1, 2)
            if os.path.exists(f"{DATA_DIR_PATH}/trip_fare_{i}.csv")
        ]

        for data_path, fare_path in zip(
            sorted(trip_data_paths), sorted(trip_fare_paths)
        ):
            TaxiConverter(
                data_path, fare_path, output_file, float(scaling_factor) or 1.0
            ).convert_to_senml_csv(CHUNK_SIZE)

    elif dataset == "FIT":
        output_file = os.environ.get("OUTPUT_FILE")

        if not output_file:
            raise ValueError("Missing required environment variables for FIT dataset")

        input_file = "/home/input_joined_fit.csv"
        create_table_fit(input_file, 1417890600020)
        convert_fit(input_file, output_file, False, float(scaling_factor) or 1)

    elif dataset == "TRAIN":
        if not output_file:
            raise ValueError("Missing required environment variables for TRAIN dataset")
        interval = float(os.environ.get("INTERVAL")) or 30
        time_bench = float(os.environ.get("DURATION")) or 60
        scaling_factor = os.environ.get("SCALING")
        convert_train(output_file, interval, time_bench)

    # Note This is a workaround, because the original sys data is not available
    elif dataset == "SYS":
        if not output_file:
            raise ValueError("Missing required environment variables for SYS dataset")
        data_dir_path: str = "/home"
        file_paths = [
            f"{data_dir_path}/File{i}.txt"
            for i in range(1, 7)
            if os.path.exists(f"{data_dir_path}/File{i}.txt")
        ]

        start_date = 0
        first_file = True
        for file in file_paths:
            converter = SysConverter(
                file,
                output_file or "/home/out.csv",
                float(scaling_factor) or 1,
                start_date,
            )
            if start_date == 0 and first_file:
                start_date = converter.get_first_date()
            elif start_date == 0 and not first_file:
                raise Exception
            converter.convert_to_senml_csv(1000)

    elif dataset == "GRID":
        file_paths: List[str] = [
            f"{DATA_DIR_PATH}/File{i}.txt"
            for i in range(1, 7)
            if os.path.exists(f"{DATA_DIR_PATH}/File{i}.txt")
        ]

        for file in file_paths:
            converter = GridConverter(
                file, output_file or "/home/out.csv", float(scaling_factor) or 1
            )
            converter.convert_to_senml_csv(CHUNK_SIZE)

    else:
        raise ValueError("No valid dataset-type given via -e DATASET=")
