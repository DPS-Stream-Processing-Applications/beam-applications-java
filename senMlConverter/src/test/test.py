import pandas as pd

def test_reading_from_taxi_converter(path):
    df = pd.read_csv(path, delimiter="|")
    for index, row in df.iterrows():
        print("First column:", row.iloc[0])
        print("Second column:", row.iloc[1])



def main():
    path = "/home/jona/Documents/Bachelor_thesis/repos/offical_repo/beam-applications-java/senMlConverter/data/test_output.csv"
    test_reading_from_taxi_converter(path)


if __name__ == "__main__":
    main()


