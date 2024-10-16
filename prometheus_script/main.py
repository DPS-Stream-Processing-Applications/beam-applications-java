import requests
import csv
from datetime import datetime, timezone, timedelta
import pytz

# NOTE: This script is build to work on chameleon cloud nodes, which use UTC-timezone.

local_tz = pytz.timezone("Europe/Berlin")
PROMETHEUS_URL = "http://localhost:9090/api/v1/query_range"


def fetch_metrics(start_time, end_time, metric_name, step="30"):
    start_time = int(start_time.timestamp())
    end_time = int(end_time.timestamp())
    query_params = {
        "query": metric_name,
        "start": start_time,
        "end": end_time,
        "step": step,
    }
    response = requests.get(PROMETHEUS_URL, params=query_params)
    response.raise_for_status()

    return response.json()


def save_to_csv_for_application_metrics(data, filename):
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        if (
            "data" in data
            and "result" in data["data"]
            and len(data["data"]["result"]) > 0
        ):
            metric_name = data["data"]["result"][0]["metric"]
            writer.writerow(["Time", metric_name])
            for point in data["data"]["result"][0]["values"]:
                timestamp = datetime.fromtimestamp(point[0], tz=timezone.utc)
                timestamp_local = timestamp.astimezone(local_tz).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                value = point[1]
                writer.writerow([timestamp_local, value])
        else:
            print("No data found for the specified metric.")


def save_to_csv_operator_metric(data, filename_prefix, date):
    for result in data["data"]["result"]:
        operator_name = result["metric"].get("operator_name", "unknown_operator")
        metric_name = result["metric"]

        filename = f"{filename_prefix}_{operator_name}_{date}.csv"

        with open(filename, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Time", metric_name])
            for point in result["values"]:
                timestamp = datetime.fromtimestamp(point[0], tz=timezone.utc)
                timestamp_local = timestamp.astimezone(local_tz).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                value = point[1]
                writer.writerow([timestamp_local, value])


def save_to_csv_operator_metric(data, filename_prefix, date):
    for result in data["data"]["result"]:
        operator_name = result["metric"].get("operator_name", "unknown_operator")
        metric_name = result["metric"]
        filename = f"{filename_prefix}_{operator_name}_{date}.csv"

        with open(filename, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Time", metric_name])
            for point in result["values"]:
                timestamp = datetime.fromtimestamp(point[0], tz=timezone.utc)
                timestamp_local = timestamp.astimezone(local_tz).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                value = point[1]
                writer.writerow([timestamp_local, value])


def save_to_csv_task_metric(data, filename_prefix, date):
    for result in data["data"]["result"]:
        operator_name = result["metric"].get("task_name", "unknown_task")
        metric_name = result["metric"]

        filename = f"{filename_prefix}_{operator_name}_{date}.csv"

        with open(filename, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Time", metric_name])

            for point in result["values"]:
                timestamp = datetime.fromtimestamp(point[0], tz=timezone.utc)
                timestamp_local = timestamp.astimezone(local_tz).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                value = point[1]
                writer.writerow([timestamp_local, value])


def main():
    observation_date = 191014
    path = "/home/cc/observations/p_f_c_40/"

    save_metric_to_csv = True

    # cluster or statefun
    framework = "cluster"
    # PRED or Train
    application = "PRED"

    # year, month, day, hour, minute, second
    local_start_time = datetime(2024, 10, 15, 16, 37)
    local_end_time = datetime(2024, 10, 15, 17, 17)
    offset = timedelta(hours=2)
    utc_start_time = local_start_time - offset
    utc_end_time = local_end_time - offset

    start_time = utc_start_time
    end_time = utc_end_time
    simple_metrics = [
        "flink_jobmanager_Status_JVM_CPU_Load",
        "flink_jobmanager_Status_JVM_CPU_Time",
        "flink_taskmanager_Status_JVM_Memory_Heap_Used",
        "flink_jobmanager_Status_JVM_Memory_Heap_Used",
        "flink_taskmanager_Status_JVM_CPU_Load",
        "flink_taskmanager_Status_JVM_CPU_Time",
    ]
    if framework == "cluster":
        simple_metrics.append(
            "flink_taskmanager_job_task_operator_at_ac_uibk_dps_streamprocessingapplications_beam_Sink_custom_latency"
        )

    for metric_name in simple_metrics:
        data = fetch_metrics(start_time, end_time, metric_name)
        if data["data"]["result"] == []:
            raise Exception(metric_name + " returned empty result")
        if save_metric_to_csv:
            save_to_csv_for_application_metrics(
                data, path + metric_name + "_" + str(observation_date) + ".csv"
            )

    operator_metrics = [
        "flink_taskmanager_job_task_operator_numRecordsIn",
        "flink_taskmanager_job_task_operator_numRecordsInPerSecond",
        "flink_taskmanager_job_task_operator_numRecordsOut",
        "flink_taskmanager_job_task_operator_numRecordsOutPerSecond",
    ]
    if not framework == "cluster":
        operator_metrics.append(
            "flink_taskmanager_job_task_operator_functions_pred_source_outLocalRate"
        )
        operator_metrics.append(
            "flink_taskmanager_job_task_operator_functions_pred_source_inRate"
        )
        if application == "PRED":
            operator_metrics.append(
                "flink_taskmanager_job_task_operator_functions_pred_mqttPublish_outLocalRate"
            )
        else:
            operator_metrics.append(
                "flink_taskmanager_job_task_operator_functions_pred_mqttPublishTrain_outLocalRate"
            )

    for metric_name in operator_metrics:
        data = fetch_metrics(start_time, end_time, metric_name)
        if data["data"]["result"] == []:
            raise Exception(metric_name + " returned empty result")
        if save_metric_to_csv:
            save_to_csv_operator_metric(data, path + metric_name, observation_date)

    task_operators = [
        "flink_taskmanager_job_task_numRecordsIn",
        "flink_taskmanager_job_task_numRecordsInPerSecond",
        "flink_taskmanager_job_task_numRecordsOut",
        "flink_taskmanager_job_task_numRecordsOutPerSecond",
        "flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
        "flink_taskmanager_job_task_idleTimeMsPerSecond",
        "flink_taskmanager_job_task_busyTimeMsPerSecond",
    ]
    for metric_name in task_operators:
        data = fetch_metrics(start_time, end_time, metric_name)
        if data["data"]["result"] == []:
            raise Exception(metric_name + " returned empty result")
        if save_metric_to_csv:
            save_to_csv_task_metric(data, path + metric_name, observation_date)


if __name__ == "__main__":
    main()
