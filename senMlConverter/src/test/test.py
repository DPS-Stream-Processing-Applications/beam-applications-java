import pandas as pd


def test_reading_from_taxi_converter(path):
    df = pd.read_csv(path, delimiter="|")
    for index, row in df.iterrows():
        timestamp = row.iloc[0]
        data_list = row.iloc[1]
        print(timestamp)
        print(data_list)
        break


def main():
    path = "/home/jona/Documents/Bachelor_thesis/repos/official_repo/beam-applications-java/senMlConverter/data/output_taxi.csv"
    test_reading_from_taxi_converter(path)


if __name__ == "__main__":
    main()
