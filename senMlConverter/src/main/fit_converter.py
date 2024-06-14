import csv

import pandas as pd


class FitConverter:
    def __init__(self, output_file: str, scaling: float):
        self.output_file: str = output_file
        self.scaling_factor: float = scaling
        self.relative_elapsed_time: int = 0
        self.subject_id = 1

    def convert_to_senml_csv(self, input_file: str, chunk_size: int):
        with open(self.output_file, "a") as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter="|",
                quoting=csv.QUOTE_MINIMAL,
                escapechar=None,
                quotechar=" ",
            )
            for chunk in pd.read_csv(
                input_file,
                chunksize=chunk_size,
                sep="\t",
                names=[
                    "subjectId",
                    "timestamp",
                    "acc_chest_x",
                    "acc_chest_y",
                    "acc_chest_z",
                    "ecg_lead_1",
                    "ecg_lead_2",
                    "acc_ankle_x",
                    "acc_ankle_y",
                    "acc_ankle_z",
                    "gyro_ankle_x",
                    "gyro_ankle_y",
                    "gyro_ankle_z",
                    "magnetometer_ankle_x",
                    "magnetometer_ankle_y",
                    "magnetometer_ankle_z",
                    "acc_arm_x",
                    "acc_arm_y",
                    "acc_arm_z",
                    "gyro_arm_x",
                    "gyro_arm_y",
                    "gyro_arm_z",
                    "magnetometer_arm_x",
                    "magnetometer_arm_y",
                    "magnetometer_arm_z",
                    "label",
                ],
            ):
                for row in chunk.itertuples(index=False):
                    elapsed_time: int = int(
                        self.relative_elapsed_time * self.scaling_factor
                    )
                    writer.writerow(
                        [
                            elapsed_time,
                            (
                                "["
                                f'{{"u":"string","n":"subjectId","vs":"subject{self.subject_id}"}},'
                                f'{{"v":"{row.acc_chest_x}","u":"m/s2","n":"acc_chest_x"}},'
                                f'{{"v":"{row.acc_chest_y}","u":"m/s2","n":"acc_chest_y"}},'
                                f'{{"v":"{row.acc_chest_z}","u":"m/s2","n":"acc_chest_z"}},'
                                f'{{"v":"{row.ecg_lead_1}","u":"mV","n":"ecg_lead_1"}},'
                                f'{{"v":"{row.ecg_lead_2}","u":"mV","n":"ecg_lead_2"}},'
                                f'{{"v":"{row.acc_ankle_x}","u":"m/s2","n":"acc_ankle_x"}},'
                                f'{{"v":"{row.acc_ankle_y}","u":"m/s2","n":"acc_ankle_y"}},'
                                f'{{"v":"{row.acc_ankle_z}","u":"m/s2","n":"acc_ankle_z"}},'
                                f'{{"v":"{row.gyro_ankle_x}","u":"deg/s","n":"gyro_ankle_x"}},'
                                f'{{"v":"{row.gyro_ankle_y}","u":"deg/s","n":"gyro_ankle_y"}},'
                                f'{{"v":"{row.gyro_ankle_z}","u":"deg/s","n":"gyro_ankle_z"}},'
                                f'{{"v":"{row.magnetometer_ankle_x}","u":"tesla","n":"magnetometer_ankle_x"}},'
                                f'{{"v":"{row.magnetometer_ankle_y}","u":"tesla","n":"magnetometer_ankle_y"}},'
                                f'{{"v":"{row.magnetometer_ankle_z}","u":"tesla","n":"magnetometer_ankle_z"}},'
                                f'{{"v":"{row.acc_arm_x}","u":"m/s2","n":"acc_arm_x"}},'
                                f'{{"v":"{row.acc_arm_y}","u":"m/s2","n":"acc_arm_y"}},'
                                f'{{"v":"{row.acc_arm_z}","u":"m/s2","n":"acc_arm_z"}},'
                                f'{{"v":"{row.gyro_arm_x}","u":"deg/s","n":"gyro_arm_x"}},'
                                f'{{"v":"{row.gyro_arm_y}","u":"deg/s","n":"gyro_arm_y"}},'
                                f'{{"v":"{row.gyro_arm_z}","u":"deg/s","n":"gyro_arm_z"}},'
                                f'{{"v":"{row.magnetometer_arm_x}","u":"tesla","n":"magnetometer_arm_x"}},'
                                f'{{"v":"{row.magnetometer_arm_y}","u":"tesla","n":"magnetometer_arm_y"}},'
                                f'{{"v":"{row.magnetometer_arm_z}","u":"tesla","n":"magnetometer_arm_z"}},'
                                f'{{"v":"{row.label}","u":"number","n":"label"}}'
                                "]"
                            ),
                        ]
                    )

                    # NOTE:
                    # To enforce a steady stream of 500 events / second as the baseline,
                    # a 2ms offset is added to each event resulting in a one second offset every 500 events.
                    self.relative_elapsed_time += 2

        # File completely read reading potentially next subject file
        self.subject_id += 1
