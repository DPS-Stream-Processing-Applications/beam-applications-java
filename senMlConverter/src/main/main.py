import sys
import os
import pandas as pd
from taxi_converter import TaxiConverter


def main(input, output):
    test = TaxiConverter(input, output)
    test.convert_to_senml_csv(1200)

def test(out):
    for chunk in pd.read_csv(out):
        print(chunk)




if __name__ == "__main__":
    #input_file = "/home/jona/Documents/Bachelor_thesis/Datasets/output_TAXI.csv"
    #output_file = "/home/jona/Documents/Bachelor_thesis/repos/offical_repo/beam-applications-java/senMlConverter/data/test_output.csv"
   
    
    input_file = os.environ.get('INPUT_FILE')
    output_file = os.environ.get('OUTPUT_FILE')
    #input_file = sys.argv[0]
    #output_file = sys.argv[1]
    main(input_file, output_file)


#command
# "/home/jona/Documents/Bachelor_thesis/Datasets/output_TAXI_small.csv"
# /home/jona/Documents/Bachelor_thesis/repos/offical_repo/beam-applications-java/senMlConverter/data/test_output.csv"
# /home/jona/Documents/Bachelor_thesis/repos/offical_repo/beam-applications-java/senMlConverter/data
#/home/jona/Documents/Bachelor_thesis/repos/offical_repo/beam-applications-java/senMlConverter/data/output_TAXI_small.csv
# docker run -v /home/jona/Documents/Bachelor_thesis/repos/offical_repo/beam-applications-java/senMlConverter/data:/app/data senml_converter /app/data/output_TAXI_small.csv /app/data/test_out.csv
# docker run --rm -v $PWD/data:/home -e INPUT_FILE="/home/output_TAXI_small.csv -e OUTPUT_FILE="/home/test_output.csv senml_converte