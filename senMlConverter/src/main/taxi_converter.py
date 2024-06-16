import csv
from datetime import datetime

import numpy as np
import pandas as pd
from pandas.core.generic import DtypeArg


def date_to_unix_timestamp(date_string: str) -> int:
    date_object = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
    unix_timestamp = date_object.timestamp()
    # Convert to milliseconds
    return int(unix_timestamp * 1000)


class TaxiConverter:
    def __init__(
        self,
        output_file: str,
        scaling_factor: float,
    ):
        self.output_file: str = output_file
        self.scaling_factor: float = scaling_factor
        self.start_timestamp: int | None = None

    def convert_to_senml_csv(
        self, input_file_data: str, input_file_fare: str, chunk_size: int
    ):
        with open(self.output_file, "a") as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter="|",
                quoting=csv.QUOTE_MINIMAL,
                escapechar=None,
                quotechar=" ",
            )

            dtypes_data: DtypeArg = {
                "medallion": np.int64,
                "hack_license": np.int64,
                "vendor_id": str,
                "rate_code": np.int64,
                "store_and_fwd_flag": str,
                "pickup_datetime": str,
                "dropoff_datetime": str,
                "passenger_count": np.int8,
                "trip_time_in_secs": np.int64,
                "trip_distance": np.float64,
                "pickup_longitude": np.float64,
                "pickup_latitude": np.float64,
                "dropoff_longitude": np.float64,
                "dropoff_latitude": np.float64,
            }

            data_chunk_iterator = pd.read_csv(
                input_file_data,
                header=0,
                chunksize=chunk_size,
                sep=",",
                names=[
                    "medallion",
                    "hack_license",
                    "vendor_id",
                    "rate_code",
                    "store_and_fwd_flag",
                    "pickup_datetime",
                    "dropoff_datetime",
                    "passenger_count",
                    "trip_time_in_secs",
                    "trip_distance",
                    "pickup_longitude",
                    "pickup_latitude",
                    "dropoff_longitude",
                    "dropoff_latitude",
                ],
                dtype=dtypes_data,
            )

            dtypes_fare: DtypeArg = {
                "medallion": np.int64,
                "hack_license": np.int64,
                "vendor_id": str,
                "pickup_datetime": str,
                "payment_type": str,
                "fare_amount": np.float64,
                "surcharge": np.float64,
                "mta_tax": np.float64,
                "tip_amount": np.float64,
                "tolls_amount": np.float64,
                "total_amount": np.float64,
            }

            fare_chunk_iterator = pd.read_csv(
                input_file_fare,
                header=0,
                chunksize=chunk_size,
                sep=",",
                names=[
                    "medallion",
                    "hack_license",
                    "vendor_id",
                    "pickup_datetime",
                    "payment_type",
                    "fare_amount",
                    "surcharge",
                    "mta_tax",
                    "tip_amount",
                    "tolls_amount",
                    "total_amount",
                ],
                dtype=dtypes_fare,
            )

            for data_chunk, fare_chunk in zip(data_chunk_iterator, fare_chunk_iterator):
                for data_row, fare_row in zip(
                    data_chunk.itertuples(index=False),
                    fare_chunk.itertuples(index=False),
                ):
                    timestamp: int = date_to_unix_timestamp(data_row.pickup_datetime)
                    if self.start_timestamp == None:
                        self.start_timestamp = timestamp

                    relative_elapsed_time_scaled: int = int(
                        (timestamp - self.start_timestamp) * self.scaling_factor
                    )

                    writer.writerow(
                        [
                            # NOTE: Casting to int to have clean millisecond value
                            relative_elapsed_time_scaled,
                            (
                                "["
                                f'{{"u":"string","n":"taxi_identifier","vs":"{data_row.medallion}"}},'
                                f'{{"u":"string","n":"hack_license","vs":"{data_row.hack_license}"}},'
                                f'{{"u":"time","n":"pickup_datetime","vs":"{data_row.pickup_datetime}"}},'
                                f'{{"u":"s","n":"trip_time_in_secs","v":"{data_row.trip_time_in_secs}"}},'
                                f'{{"u":"m","n":"trip_distance","v":"{data_row.trip_distance}"}},'
                                f'{{"u":"deg","n":"pickup_longitude","v":"{data_row.pickup_longitude}"}},'
                                f'{{"u":"deg","n":"pickup_latitude","v":"{data_row.pickup_latitude}"}},'
                                f'{{"u":"deg","n":"dropoff_longitude","v":"{data_row.dropoff_longitude}"}},'
                                f'{{"u":"deg","n":"dropoff_latitude","v":"{data_row.dropoff_latitude}"}},'
                                f'{{"u":"payment_type","n":"payment_type","vs":"{fare_row.payment_type}"}},'
                                f'{{"u":"dollar","n":"fare_amount","v":"{fare_row.fare_amount}"}},'
                                f'{{"u":"%","n":"surcharge","v":"{fare_row.surcharge}"}},'
                                f'{{"u":"%","n":"mta_tax","v":"{fare_row.mta_tax}"}},'
                                f'{{"u":"dollar","n":"tip_amount","v":"{fare_row.tip_amount}"}},'
                                f'{{"u":"dollar","n":"tolls_amount","v":"{fare_row.tolls_amount}"}},'
                                f'{{"u":"dollar","n":"total_amount","v":"{fare_row.total_amount}"}}'
                                "]"
                            ),
                        ]
                    )
