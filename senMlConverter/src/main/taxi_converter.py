import csv
from datetime import datetime

import pandas as pd


def date_to_unix_timestamp(date_string: str) -> int:
    date_object = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
    unix_timestamp = date_object.timestamp()
    return int(unix_timestamp)


class TaxiConverter:
    def __init__(
        self,
        input_file_data: str,
        input_file_fare: str,
        output_file: str,
        scaling_factor: float,
    ):
        self.input_file_data: str = input_file_data
        self.input_file_fare: str = input_file_fare
        self.outputFile: str = output_file
        self.scaling_factor: float = scaling_factor

    def convert_to_senml_csv(self, chunk_size):
        start_timestamp: int = 0
        with open(self.outputFile, "w", newline="") as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter="|",
                quoting=csv.QUOTE_NONE,
            )

            for chunk in pd.read_csv(
                self.input_file_data, chunksize=chunk_size, skipinitialspace=True
            ):
                for j, row in chunk.iterrows():
                    timestamp: int = int(date_to_unix_timestamp(row["pickup_datetime"]))
                    # NOTE:
                    # We want the event stream to always start from 0.
                    # The first row is
                    if j == 1:
                        start_timestamp = timestamp

                    relative_elapsed_time_scaled: float = (
                        timestamp - start_timestamp
                    ) * self.scaling_factor

                    writer.writerow(
                        [
                            # NOTE: Casting to int to have clean millisecond value
                            int(relative_elapsed_time_scaled),
                            (
                                "["
                                f'{{"u": "string","n": "taxi_identifier","vs": {row["taxi_identifier"]}"}},'
                                f'{{"u": "string","n": "hack_license","vs": "{row["hack_license"]}}},'
                                f'{{"u": "time", "n": "pickup_datetime","vs": "{row["pickup_datetime"]}"}},'
                                f'{{"u": "s","n": "trip_time_in_secs","v": "{row["trip_time_in_secs"]}"}},'
                                f'{{"u": "m","n": "trip_distance","v": "{row["trip_distance"]}"}},'
                                f'{{"u": "deg","n": "pickup_longitude","v": "{row["pickup_longitude"]}"}},'
                                f'{{"u": "deg","n": "pickup_latitude","v": "{row["pickup_latitude"]}"}},'
                                f'{{"u": "deg","n": "dropoff_longitude","v": "{row["dropoff_longitude"]}"}},'
                                f'{{"u": "deg","n": "dropoff_latitude","v": "{row["dropoff_latitude"]}"}},'
                                f'{{"u": "payment_type","n": "payment_type","vs": "{row["payment_type"]}"}},'
                                f'{{"u": "dollar","n": "fare_amount","v": "{row["fare_amount"]}"}},'
                                f'{{"u": "%","n": "surcharge","v": "{row["surcharge"]}"}},'
                                f'{{"u": "%","n": "mta_tax","v": "{row["mta_tax"]}"}},'
                                f'{{"u": "dollar","n": "tip_amount","v": "{["tip_amount"]}"}},'
                                f'{{"u": "dollar","n": "tolls_amount","v": "{row["tolls_amount"]}"}},'
                                f'{{"u": "dollar","n": "total_amount","v": "{row["total_amount"]}"}}'
                                "]"
                            ),
                        ]
                    )
