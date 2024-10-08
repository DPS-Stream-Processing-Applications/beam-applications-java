import csv

import numpy as np
import pandas as pd
from pandas.core.generic import DtypeArg


class GridConverter:
    def __init__(self, outputFile: str, scaling_factor: float):
        self.output_file: str = outputFile
        self.scaling_factor: float = scaling_factor
        self.start_timestamp: int | None = None

    def convert_to_senml_csv(self, input_file: str, chunk_size: int):
        with open(self.output_file, "a") as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter="|",
                quoting=csv.QUOTE_MINIMAL,
                escapechar=None,
                quotechar=" ",
            )

            dtypes: DtypeArg = {
                "UNIX_timestamp": np.int64,
                "id": np.float64,
                "value": np.float64,
            }

            for chunk in pd.read_csv(
                input_file,
                chunksize=chunk_size,
                sep=" ",
                names=["UNIX_timestamp", "id", "value"],
                dtype=dtypes,
            ):
                for row in chunk.itertuples(index=False):
                    if self.start_timestamp == None:
                        self.start_timestamp = row.UNIX_timestamp

                    relative_elapsed_time: int = int(
                        (row.UNIX_timestamp - self.start_timestamp)
                        * self.scaling_factor
                    )
                    writer.writerow(
                        [
                            relative_elapsed_time,
                            (
                                r"["
                                f'{{"n":"id","u":"double","v":{row.id}}},'
                                f'{{"n":"grid_measurement","u":"double","v":{row.value}}},'
                                f'{{"n":"timestamp","u":"s","v":{row.UNIX_timestamp}}}'
                                r"]"
                            ),
                        ]
                    )
