import sys
import os
import pandas as pd
from taxi_converter import TaxiConverter
from fit_converter import FitConverter
from taxi_modifier import (
    filter_date,
    rename_cols,
    drop_cols_fare,
    drop_cols_trip,
    merge_dfs,
    change_header_order_df,
)
from fit_modifier import create_table_fit


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


def convert_taxi(input, output):
    converter = TaxiConverter(input, output)
    converter.convert_to_senml_csv(1200)


def convert_fit(input, output):
    converter = FitConverter(input, output)
    converter.convert_to_senml_csv(10000)


if __name__ == "__main__":
    # input_file = "/home/jona/Documents/Bachelor_thesis/Datasets/output_TAXI.csv"
    # output_file = "/home/jona/Documents/Bachelor_thesis/repos/offical_repo/beam-applications-java/senMlConverter/data/test_output.csv"
    dataset = os.environ.get("DATASET")
    if dataset == "TAXI":
        input_fare = os.environ.get("INPUT_FILE_FARE")
        input_trip = os.environ.get("INPUT_FILE_TRIP")
        output_file = os.environ.get("OUTPUT_FILE")

        if not (input_fare and input_trip and output_file):
            raise ValueError("Missing required environment variables for TAXI dataset")

        create_table_taxi(input_fare, input_trip)
        input_file = "/home/input_joined.csv"
        convert_taxi(input_file, output_file)

    elif dataset == "FIT":
        output_file = os.environ.get("OUTPUT_FILE")

        if not output_file:
            raise ValueError("Missing required environment variables for FIT dataset")

        input_file = "/home/input_joined_fit.csv"
        create_table_fit(input_file, 1417890600020)
        convert_fit(input_file, output_file)

    else:
        raise ValueError("No valid datasettype given via -e DATASET=")
