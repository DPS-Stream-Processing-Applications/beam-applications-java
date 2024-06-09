import csv
from datetime import datetime

import pandas as pd
from converter import Converter


def date_to_unix_timestamp(date_string):
    date_object = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
    unix_timestamp = date_object.timestamp()
    return int(unix_timestamp)


class TaxiConverter(Converter):
    def __init__(self, inputFile, outputFile, scaling_factor):
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.total_entries = 0
        self.scaling_factor = float(scaling_factor)

    def convert_to_senml_csv(self, chunk_size):
        with open(self.outputFile, "w", newline="") as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter="|",
                quotechar="",
                quoting=csv.QUOTE_NONE,
                escapechar=" ",
            )
            set_first_timestamp = True
            first_timestamp = 0
            for chunk in pd.read_csv(self.inputFile, chunksize=chunk_size):
                start_timestamp: int = 0
                for j, row in chunk.iterrows():
                    list_senml = list()
                    senml_string = (
                        "["
                        + '{"u": "string","n": "taxi_identifier","vs": "'
                        + str(row["taxi_identifier"])
                        + '"},'
                        + '{"u": "string","n": "hack_license","vs": "'
                        + str(row[" hack_license"])
                        + '"},'
                        + '{"u": "time", "n": "pickup_datetime","vs": "'
                        + str(row[" pickup_datetime"])
                        + '"},'
                        + '{"u": "s","n": "trip_time_in_secs","v": "'
                        + str(row[" trip_time_in_secs"])
                        + '"},'
                        + '{"u": "m","n": "trip_distance","v": "'
                        + str(row[" trip_distance"])
                        + '"},'
                        + '{"u": "deg","n": "pickup_longitude","v": "'
                        + str(row[" pickup_longitude"])
                        + '"},'
                        + '{"u": "deg","n": "pickup_latitude","v": "'
                        + str(row[" pickup_latitude"])
                        + '"},'
                        + '{"u": "deg","n": "dropoff_longitude","v": "'
                        + str(row[" dropoff_longitude"])
                        + '"},'
                        + '{"u": "deg","n": "dropoff_latitude","v": "'
                        + str(row[" dropoff_latitude"])
                        + '"},'
                        + '{"u": "payment_type","n": "payment_type","vs": "'
                        + str(row[" payment_type"])
                        + '"},'
                        + '{"u": "dollar","n": "fare_amount","v": "'
                        + str(row[" fare_amount"])
                        + '"},'
                        + '{"u": "%","n": "surcharge","v": "'
                        + str(row[" surcharge"])
                        + '"},'
                        + '{"u": "%","n": "mta_tax","v": "'
                        + str(row[" mta_tax"])
                        + '"},'
                        + '{"u": "dollar","n": "tip_amount","v": "'
                        + str(row[" tip_amount"])
                        + '"},'
                        + '{"u": "dollar","n": "tolls_amount","v": "'
                        + str(row[" tolls_amount"])
                        + '"},'
                        + '{"u": "dollar","n": "total_amount","v": "'
                        + str(row[" total_amount"])
                        + '"}'
                        + "]"
                    )
                    list_senml.append(senml_string)

                    if j == 1:
                        start_timestamp = int(row["UNIX_timestamp"])

                    relative_elapsed_time = (
                        int(row["UNIX_timestamp"]) - start_timestamp
                    ) * self.scaling_factor

                    writer.writerow(
                        [
                            relative_elapsed_time,
                            (list_senml[0]),
                        ]
                    )

    def converter_to_senml_riotbench_csv(self, chunk_size):
        with open(self.outputFile, "w", newline="") as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter="|",
                quotechar="",
                quoting=csv.QUOTE_NONE,
                escapechar=" ",
            )

            for chunk in pd.read_csv(self.inputFile, chunksize=chunk_size):
                for index, row in chunk.iterrows():
                    list_senml = list()
                    timestamp = str(date_to_unix_timestamp(row[" pickup_datetime"]))
                    senml_string = (
                        '{"e":['
                        + '{"u": "string","n": "taxi_identifier","sv": "'
                        + str(row["taxi_identifier"])
                        + "},"
                        + '{"u": "string","n": "hack_license","sv": "'
                        + str(row[" hack_license"])
                        + '"},'
                        + '{"u": "time", "n": "pickup_datetime","sv": "'
                        + str(row[" pickup_datetime"])
                        + '"},'
                        + '{"v": "'
                        + str(row[" trip_time_in_secs"])
                        + '", "u": "s","n": "trip_time_in_secs"},'
                        + '{"v": "'
                        + str(row[" trip_distance"])
                        + '", "u": "m","n": "trip_distance"},'
                        + '{"v": "'
                        + str(row[" pickup_longitude"])
                        + '", "u": "deg","n": "pickup_longitude"},'
                        + '{"v": "'
                        + str(row[" pickup_latitude"])
                        + '","u": "deg","n": "pickup_latitude"},'
                        + '{"v": "'
                        + str(row[" dropoff_longitude"])
                        + '","u": "deg","n": "dropoff_longitude"},'
                        + '{"v": "'
                        + str(row[" dropoff_latitude"])
                        + '","u": "deg","n": "dropoff_latitude"},'
                        + '{"u": "payment_type","n": "payment_type""sv": "'
                        + str(row[" payment_type"])
                        + '"},'
                        + '{"v": "'
                        + str(row[" fare_amount"])
                        + '","u": "dollar","n": "fare_amount"},'
                        + '{"v": "'
                        + str(row[" surcharge"])
                        + '","u": "%","n": "surcharge"},'
                        + '{"v": "'
                        + str(row[" mta_tax"])
                        + '","u": "%","n": "mta_tax"},'
                        + '{"v": "'
                        + str(row[" tip_amount"])
                        + '","u": "dollar","n": "tip_amount"},'
                        + '{"v": "'
                        + str(row[" tolls_amount"])
                        + '","u": "dollar","n": "tolls_amount"},'
                        + '{"v": "'
                        + str(row[" total_amount"])
                        + '","u": "dollar","n": "total_amount"}'
                        + '], "bt":'
                        + timestamp
                        + "}"
                    )
                    list_senml.append(senml_string)
                    writer.writerow(
                        [
                            timestamp,
                            (list_senml[0]),
                        ]
                    )

    def convert_to_senml_csv_dataframe(self, chunk_size):
        senml_df = pd.DataFrame(columns=["senml_entry"])

        for chunk in pd.read_csv(self.inputFile, chunksize=chunk_size):
            for index, row in chunk.iterrows():
                senml_entry = (
                    '{ "u": "string","n": "taxi_identifier", "sv": '
                    + str(row["taxi_identifier"])
                    + "},"
                )
                senml_df = pd.concat(
                    [senml_df, pd.DataFrame({"senml_entry": [senml_entry]})],
                    ignore_index=True,
                )

        senml_df.to_csv(self.outputFile, encoding="utf-8")

    def convert_to_json(self):
        pass

    def print_first_line(self):
        chunk_size = 10000
        for chunk in pd.read_csv(self.inputFile, chunksize=chunk_size):
            for index, row in chunk.iterrows():
                print(row)
                self.total_entries += 1

    def print_total_length(self):
        return self.total_entries
