import os
import sys

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

    merged_df = merge_dfs(cleaned_trip, cleaned_fare)
    ordered_df = change_header_order_df(merged_df, order)
    ordered_df.to_csv(output_file, index=False)


def convert_taxi(input, output, use_riotbench_format, scaling_factor):
    converter = TaxiConverter(input, output, scaling_factor)
    if use_riotbench_format:
        converter.converter_to_senml_riotbench_csv(10000)
    else:
        converter.convert_to_senml_csv(10000)


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
    dataset = os.environ.get("DATASET")
    scaling_factor = os.environ.get("SCALING")
    use_riotbench_format = False
    output_file = os.environ.get("OUTPUT_FILE")

    if not dataset:
        raise ValueError("Missing required environment variable DATASET")
    elif not scaling_factor and not dataset == "TRAIN":
        raise ValueError("Missing required environment variable SCALING")
    elif not output_file:
        raise ValueError("Missing required environment variable OUTPUT_FILE")

    if dataset == "TAXI":
        input_fare = os.environ.get("INPUT_FILE_FARE")
        input_trip = os.environ.get("INPUT_FILE_TRIP")

        if not (input_fare and input_trip and output_file):
            raise ValueError("Missing required environment variables for TAXI dataset")

        create_table_taxi(input_fare, input_trip)
        input_file = "/home/input_joined.csv"
        convert_taxi(input_file, output_file, use_riotbench_format, scaling_factor)

    elif dataset == "FIT":
        output_file = os.environ.get("OUTPUT_FILE")

        if not output_file:
            raise ValueError("Missing required environment variables for FIT dataset")

        input_file = "/home/input_joined_fit.csv"
        create_table_fit(input_file, 1417890600020)
        convert_fit(
            input_file, output_file, use_riotbench_format, float(scaling_factor) or 1
        )

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
        # NOTE: `data` directory should be mounted as `/home`
        data_dir_path: str = "/home"
        file_paths = [
            f"{data_dir_path}/File{i}.txt"
            for i in range(1, 7)
            if os.path.exists(f"{data_dir_path}/File{i}.txt")
        ]

        for file in file_paths:
            converter = GridConverter(
                file, output_file or "/home/out.csv", float(scaling_factor) or 1
            )
            converter.convert_to_senml_csv(1000)

    else:
        raise ValueError("No valid dataset-type given via -e DATASET=")
