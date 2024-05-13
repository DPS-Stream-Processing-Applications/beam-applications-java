import csv


def write_into_csv_file_create(input_file, output_file, start_value):
    with open(input_file, "r") as f:
        lines = f.readlines()

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)

        # Need 26 Columns (subject, timestamp) + 24 values
        writer.writerow(
            [
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
            ]
        )

        for line in lines:
            values = line.strip().split()
            values.insert(0, "subject1")
            new_value = start_value + 1000
            values.insert(1, str(new_value))
            start_value = new_value
            writer.writerow(values)
    return start_value


def append_to_csv_file(input, output, subject_id, start_value):
    with open(input, "r") as f:
        lines = f.readlines()
    with open(output, "a", newline="") as f:
        writer = csv.writer(f)
        for line in lines:
            values = line.strip().split()
            values.insert(0, subject_id)
            new_value = start_value + 1000
            values.insert(1, str(new_value))
            start_value = new_value
            writer.writerow(values)
    return start_value


def create_table_fit(output_file, start_value):
    input_file = "/home/MHEALTHDATASET/mHealth_subject1.log"
    start = write_into_csv_file_create(input_file, output_file, start_value)
    subject_list = [
        "subject2",
        "subject3",
        "subject4",
        "subject5",
        "subject6",
        "subject7",
        "subject8",
        "subject9",
        "subject10",
    ]
    for subject in subject_list:
        input_file = "/home/MHEALTHDATASET/mHealth_" + subject + ".log"
        new_start = append_to_csv_file(input_file, output_file, subject, start)
        start = new_start
