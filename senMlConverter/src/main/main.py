import os
from typing import List

from fit_converter import FitConverter
from grid_converter import GridConverter
from sys_converter import SysConverter
from taxi_converter import TaxiConverter
from train_converter import TrainConverter


def convert_train(output_file, interval, time_bench):
    converter = TrainConverter(output_file, interval, time_bench)
    converter.convert_to_senml_csv(10000)


if __name__ == "__main__":
    dataset: str | None = os.environ.get("DATASET")
    output_file: str | None = os.environ.get("OUTPUT_FILE")
    scaling: str | None = os.environ.get("SCALING")
    scaling_factor: float = float(scaling) if scaling else 1.0

    if dataset == None:
        raise ValueError("Missing required environment variable DATASET")
    if output_file == None:
        raise ValueError("Missing required environment variable OUTPUT_FILE")

    # NOTE: The User mounts `/home` as the volume where the data is stored.
    DATA_DIR_PATH: str = "/home"
    CHUNK_SIZE: int = 10000

    if dataset == "TAXI":
        trip_data_paths: List[str] = [
            f"{DATA_DIR_PATH}/trip_data_{i}.csv"
            for i in range(1, 2)
            if os.path.exists(f"{DATA_DIR_PATH}/trip_data_{i}.csv")
        ]
        trip_fare_paths: List[str] = [
            f"{DATA_DIR_PATH}/trip_fare_{i}.csv"
            for i in range(1, 2)
            if os.path.exists(f"{DATA_DIR_PATH}/trip_fare_{i}.csv")
        ]

        converter = TaxiConverter(output_file, scaling_factor)
        for data_path, fare_path in zip(
            sorted(trip_data_paths), sorted(trip_fare_paths)
        ):
            converter.convert_to_senml_csv(data_path, fare_path, CHUNK_SIZE)

    elif dataset == "FIT":
        file_paths: List[str] = [
            f"{DATA_DIR_PATH}/mHealth_subject{i}.log"
            for i in range(1, 11)
            if os.path.exists(f"{DATA_DIR_PATH}/mHealth_subject{i}.log")
        ]

        converter = FitConverter(output_file, scaling_factor)
        for file in file_paths:
            converter.convert_to_senml_csv(file, CHUNK_SIZE)

    elif dataset == "GRID":
        file_paths: List[str] = [
            f"{DATA_DIR_PATH}/File{i}.txt"
            for i in range(1, 7)
            if os.path.exists(f"{DATA_DIR_PATH}/File{i}.txt")
        ]

        converter = GridConverter(output_file, scaling_factor)
        for file in file_paths:
            converter.convert_to_senml_csv(file, CHUNK_SIZE)

    elif dataset == "TRAIN":
        interval = float(os.environ.get("INTERVAL")) or 30
        time_bench = float(os.environ.get("DURATION")) or 60
        scaling_factor = os.environ.get("SCALING")
        convert_train(output_file, interval, time_bench)

    # Note This is a workaround, because the original sys data is not available
    elif dataset == "SYS":
        if not output_file:
            raise ValueError("Missing required environment variables for SYS dataset")
        data_dir_path: str = "/home"
        file_paths = [
            f"{data_dir_path}/File{i}.txt"
            for i in range(1, 7)
            if os.path.exists(f"{data_dir_path}/File{i}.txt")
        ]

        start_date = 0
        first_file = True
        for file in file_paths:
            converter = SysConverter(
                file,
                output_file or "/home/out.csv",
                float(scaling_factor) or 1,
                start_date,
            )
            if start_date == 0 and first_file:
                start_date = converter.get_first_date()
            elif start_date == 0 and not first_file:
                raise Exception
            converter.convert_to_senml_csv(1000)

    else:
        raise ValueError("No valid dataset-type given via -e DATASET=")
