import csv
import pandas as pd
from converter import Converter


class FitConverter(Converter):
    def __init__(self, inputFile, outputFile):
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.total_entries = 0

    def convert_to_senml_csv(self, chunk_size):
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
                    senml_string = (
                        "["
                        + '{"u": "string","n": "subjectId","sv": "'
                        + str(row["subjectId"])
                        + '"'
                        + "},"
                        + '{"v": "'
                        + str(row["acc_chest_x"])
                        + '"'
                        + ' "u": "m\/s2","n": "acc_chest_x"'
                        + "},"
                        + '{"v": "'
                        + str(row["acc_chest_y"])
                        + '"'
                        + ' "u": "m\/s2","n": "acc_chest_y"'
                        + "},"
                        + '{"v": "'
                        + str(row["acc_chest_z"])
                        + '"'
                        + ' "u": "m\/s2","n": "acc_chest_z"'
                        + "},"
                        + '{"v": "'
                        + str(row["ecg_lead_1"])
                        + '"'
                        + ' "u": "mV","n": "ecg_lead_1"'
                        + "},"
                        + '{"v": "'
                        + str(row["ecg_lead_2"])
                        + '"'
                        + ' "u": "mV","n": "ecg_lead_2"'
                        + "},"
                        + '{"v": "'
                        + str(row["acc_ankle_x"])
                        + '"'
                        + ' "u": "m\/s2","n": "acc_ankle_x"'
                        + "},"
                        + '{"v": "'
                        + str(row["acc_ankle_y"])
                        + '"'
                        + ' "u": "m\/s2","n": "acc_ankle_y"'
                        + "},"
                        + '{"v": "'
                        + str(row["acc_ankle_z"])
                        + '"'
                        + ' "u": "m\/s2","n": "acc_ankle_z"'
                        + "},"
                        + '{"v": "'
                        + str(row["gyro_ankle_x"])
                        + '"'
                        + ' "u": "deg\/s","n": "gyro_ankle_x"'
                        + "},"
                        + '{"v": "'
                        + str(row["gyro_ankle_y"])
                        + '"'
                        + ' "u": "deg\/s","n": "gyro_ankle_y"'
                        + "},"
                        + '{"v": "'
                        + str(row["gyro_ankle_z"])
                        + '"'
                        + ' "u": "deg\/s","n": "gyro_ankle_z"'
                        + "},"
                        + '{"v": "'
                        + str(row["magnetometer_ankle_x"])
                        + '"'
                        + ' "u": "tesla","n": "magnetometer_ankle_x"'
                        + "},"
                        + '{"v": "'
                        + str(row["magnetometer_ankle_y"])
                        + '"'
                        + ' "u": "tesla","n": "magnetometer_ankle_y"'
                        + "},"
                        + '{"v": "'
                        + str(row["magnetometer_ankle_z"])
                        + '"'
                        + ' "u": "tesla","n": "magnetometer_ankle_z"'
                        + "},"
                        + '{"v": "'
                        + str(row["acc_arm_x"])
                        + '"'
                        + ' "u": "m\/s2","n": "acc_arm_x"'
                        + "},"
                        + '{"v": "'
                        + str(row["acc_arm_y"])
                        + '"'
                        + ' "u": "m\/s2","n": "acc_arm_y"'
                        + "},"
                        + '{"v": "'
                        + str(row["acc_arm_z"])
                        + '"'
                        + ' "u": "m\/s2","n": "acc_arm_z"'
                        + "},"
                        + '{"v": "'
                        + str(row["gyro_arm_x"])
                        + '"'
                        + ' "u": "deg\/s","n": "gyro_arm_x"'
                        + "},"
                        + '{"v": "'
                        + str(row["gyro_arm_y"])
                        + '"'
                        + ' "u": "deg\/s","n": "gyro_arm_y"'
                        + "},"
                        + '{"v": "'
                        + str(row["gyro_arm_z"])
                        + '"'
                        + ' "u": "deg\/s","n": "gyro_arm_z"'
                        + "},"
                        + '{"v": "'
                        + str(row["magnetometer_arm_x"])
                        + '"'
                        + ' "u": "tesla","n": "magnetometer_arm_x"'
                        + "},"
                        + '{"v": "'
                        + str(row["magnetometer_arm_y"])
                        + '"'
                        + ' "u": "tesla","n": "magnetometer_arm_y"'
                        + "},"
                        + '{"v": "'
                        + str(row["magnetometer_arm_z"])
                        + '"'
                        + ' "u": "tesla","n": "magnetometer_arm_z"'
                        + "},"
                        + '{"v": "'
                        + str(row["label"])
                        + '"'
                        + ' "u": "number","n": "label"'
                        + "}"
                        + "]"
                    )
                    list_senml.append(senml_string)
                    writer.writerow([row["timestamp"], (list_senml[0])])
