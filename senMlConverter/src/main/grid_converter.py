import csv

import pandas as pd
from converter import Converter


class GridConverter(Converter):
    def __init__(self, inputFile: str, outputFile: str, scaling_factor: float):
        self.inputFile: str = inputFile
        self.outputFile: str = outputFile
        self.scaling_factor: float = scaling_factor

    def convert_to_senml_csv(self, chunk_size):
        with open(self.outputFile, "w", newline="") as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter="|",
                quotechar="",
                quoting=csv.QUOTE_NONE,
                escapechar=" ",
            )

            for chunk in pd.read_csv(
                self.inputFile,
                chunksize=chunk_size,
                sep=" ",
                names=["UNIX_timestamp", "id", "value"],
            ):
                start_timestamp: int = 0
                for j, row in chunk.iterrows():
                    # NOTE::
                    # Item format:
                    # (UNIX_timestamp, id, value)
                    relative_timestamp = start_timestamp + (
                        row["UNIX_timestamp"] / self.scaling_factor
                    )
                    writer.writerow(
                        [
                            int(relative_timestamp),
                            (
                                "["
                                f'{{"n": "id", "u": "double", "v": {row["id"]}}},'
                                f'{{"n": "grid_measurement", "u": "double", "v": {row["value"]}}},'
                                f'{{"n": "timestamp", "u": "s", "v": {row["UNIX_timestamp"]}}}'
                                "]"
                            ),
                        ]
                    )

    def converter_to_senml_riotbench_csv(self):
        # INFO:
        # This dataset is not going to be used for the `TRAIN` and `PRED` workflows,
        # because there is only a single field for each datapoint.
        # Therefore, the original riotbench format for csv can be skiped.
        pass
