import csv

import pandas as pd
from converter import Converter


class GridConverter(Converter):
    def convert_to_senml_csv(self, chunk_size):
        pass


def converter_to_senml_riotbench_csv(self):
    # INFO:
    # This dataset is not going to be used for the `TRAIN` and `PRED` workflows,
    # because there is only a single field for each datapoint.
    # Therefore, the original riotbench format for csv can be skiped.
    pass
