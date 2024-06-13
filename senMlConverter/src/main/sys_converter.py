import csv
import random

import pandas as pd


class SysConverter:
    def __init__(self, inputFile, outputFile, scaling, start_date):
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.scaling_factor = scaling
        self.first_date = start_date

    def generate_random_floats(self, low, up):
        if low >= up:
            raise ValueError("The lower bound must be less than the upper bound.")
        return random.uniform(low, up)

    def generate_random_ints(self, low, up):
        if low > up:
            raise ValueError(
                "The lower bound must be less than or equal to the upper bound."
            )
        return random.randint(low, up)

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
            for chunk in pd.read_csv(
                self.inputFile,
                chunksize=chunk_size,
                sep=" ",
                names=["id", "timecode", "value"],
            ):
                for j, row in chunk.iterrows():
                    delay_stamp = 0
                    if self.first_date == 0:
                        first_timestamp = decode_datetime_to_unix(
                            str(int(row["timecode"]))
                        )
                        self.first_date = first_timestamp
                        delay_stamp = 5 * 1000
                    else:
                        delay_stamp = (
                            decode_datetime_to_unix(str(int(row["timecode"])))
                            - first_timestamp
                        )
                        if delay_stamp == 0:
                            delay_stamp = 5
                        delay_stamp = delay_stamp * 1000
                        if delay_stamp != 5000:
                            delay_stamp = int(delay_stamp / self.scaling_factor)
                        if delay_stamp < 0:
                            delay_stamp = delay_stamp * -1

                    source = "ci4lrerertvs" + str(int(row["id"]))
                    longitude = self.generate_random_floats(1, 200)
                    latitude = self.generate_random_floats(1, 200)
                    temperature = round(self.generate_random_floats(0, 130), 1)
                    humidity = round(self.generate_random_floats(1, 99), 1)
                    light = self.generate_random_ints(0, 3000)
                    dust = round(self.generate_random_floats(100, 5000), 2)
                    airquality = self.generate_random_ints(12, 200)

                    writer.writerow(
                        [
                            int(delay_stamp),
                            (
                                "["
                                f'{{"u":"string","n":"source","vs":"{source}"}},'
                                f'{{"v":"{longitude}","u":"lon","n":"longitude"}},'
                                f'{{"v":"{latitude}","u":"lat","n":"latitude"}},'
                                f'{{"v":"{temperature}","u":"far","n":"temperature"}},'
                                f'{{"v":"{humidity}","u":"per","n":"humidity"}},'
                                f'{{"v":"{light}","u":"per","n":"light"}},'
                                f'{{"v":"{dust}","u":"per","n":"dust"}},'
                                f'{{"v":"{airquality}","u":"per","n":"airquality_raw"}}'
                                "]"
                            ),
                        ]
                    )

    def converter_to_senml_riotbench_csv(self, chunk_size):
        pass

    def get_first_date(self):
        return self.first_date
