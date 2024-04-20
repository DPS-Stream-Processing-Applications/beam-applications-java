from taxi_converter import TaxiConverter


def main(input, output):
    test = TaxiConverter(input, output)
    test.convert_to_senml_csv(12)


if __name__ == "__main__":
    input_file = "/home/jona/Documents/Bachelor_thesis/Datasets/output_TAXI_small.csv"
    output_file = "/home/jona/Documents/Bachelor_thesis/repos/offical_repo/beam-applications-java/senMlConverter/data/test_output.csv"
    main(input_file, output_file)
