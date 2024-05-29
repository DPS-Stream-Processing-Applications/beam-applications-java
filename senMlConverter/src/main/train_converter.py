from converter import Converter
import csv
import pandas as pd
import random


class TrainConverter(Converter):

    def __init__(self, outputFile, interval, time_benchmark):
        self.outputFile = outputFile
        self.interval = interval
        self.time_benchmark = time_benchmark

    def convert_to_senml_csv(self, chunk_size):
        with open(self.outputFile, "w", newline="") as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter="|",
                quotechar="",
                quoting=csv.QUOTE_NONE,
                escapechar=" ",
            )
            timestamp_date = 1443033000

            number_events = int(self.time_benchmark / self.interval)
            # convert min to ms
            timestamp = int(self.interval) * 60 * 1000
            for i in range(number_events):
                list_senml = list()
                rowStart = random.randint(100, 1422748800000)
                rowEnd = random.randint(rowStart + 1, 1522748800000)
                senml_string = (
                    str(timestamp_date) + "," + str(rowStart) + "," + str(rowEnd)
                )
                list_senml.append(senml_string)
                writer.writerow([timestamp, (list_senml[0])])
                timestamp = timestamp * 2
                timestamp_date = timestamp_date + random.randint(1000, 100000)

    def converter_to_senml_riotbench_csv(self, chunk_size):
        pass
