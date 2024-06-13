import csv

import numpy as np
import pandas as pd
from pandas.core.generic import DtypeArg


class GridConverter:
    def __init__(self, inputFile: str, outputFile: str, scaling_factor: float):
        self.inputFile: str = inputFile
        self.outputFile: str = outputFile
        self.scaling_factor: float = scaling_factor

    def convert_to_senml_csv(self, chunk_size: int):
        with open(self.outputFile, "w") as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter="|",
                quoting=csv.QUOTE_MINIMAL,
                escapechar=None,
                quotechar=" ",
            )
            start_timestamp: int | None = None
            dtypes: DtypeArg = {
                "UNIX_timestamp": np.int64,
                "id": np.float64,
                "value": np.float64,
            }

            for chunk in pd.read_csv(
                self.inputFile,
                chunksize=chunk_size,
                sep=" ",
                names=["UNIX_timestamp", "id", "value"],
                dtype=dtypes,
                # engine='pyarrow'
            ):
                for _, row in chunk.iterrows():
                    # NOTE:
                    # The event stream should start immediately with the first event.
                    # To calculate the relative elapsed time between the initial event and the following,
                    # The original start time needs to be saved. This is the timestamp of the very first row.
                    if start_timestamp == None:
                        start_timestamp = row["UNIX_timestamp"]

                    relative_elapsed_time: int = int(
                        (row["UNIX_timestamp"] - start_timestamp) * self.scaling_factor
                    )

                    writer.writerow(
                        [
                            relative_elapsed_time,
                            (
                                r"["
                                f'{{"n":"id","u":"double","v":"{row["id"]}"}},'
                                f'{{"n":"grid_measurement","u":"double","v":"{row["value"]}"}},'
                                f'{{"n":"timestamp","u":"s","v":"{int(row["UNIX_timestamp"])}"}}'
                                r"]"
                            ),
                        ]
                    )
