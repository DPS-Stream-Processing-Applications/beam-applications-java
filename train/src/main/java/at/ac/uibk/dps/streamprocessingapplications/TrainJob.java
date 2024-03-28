package at.ac.uibk.dps.streamprocessingapplications;

import at.ac.uibk.dps.streamprocessingapplications.beam.*;
import at.ac.uibk.dps.streamprocessingapplications.entity.*;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentClass;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentParser;
import java.io.*;
import java.util.Properties;
import java.util.Scanner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class TrainJob {

    public static long countLines(String resourceFileName) {
        long lines = 0;
        try (InputStream inputStream = TrainJob.class.getResourceAsStream(resourceFileName)) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(
                    "Error when counting lines in resource file: " + e.getMessage());
        }
        return lines;
    }

    public static String checkDataType(String fileName) {
        if (fileName.contains("SYS") | fileName.contains("CITY")) {
            return "SYS";
        } else if (fileName.contains("FIT")) {
            return "FIT";
        } else if (fileName.contains("TAXI")) {
            return "TAXI";

        } else if (fileName.contains("GRID")) {
            return "GRID";
        }
        return null;
    }

    public static void test() {
        // Load a resource file as an input stream
        InputStream inputStream =
                TrainJob.class.getResourceAsStream(
                        "/resources/datasets/TAXI_sample_data_senml.csv");
        if (inputStream != null) {
            // Process the input stream (e.g., read content)
            // Example: Read content line by line
            try (Scanner scanner = new Scanner(inputStream)) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    System.out.println(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("Resource not found!");
        }
    }

    public static void main(String[] args) throws Exception {

        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }

        String logFilePrefix =
                argumentClass.getTopoName()
                        + "-"
                        + argumentClass.getExperiRunId()
                        + "-"
                        + argumentClass.getScalingFactor()
                        + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String taskPropFilename = argumentClass.getTasksPropertiesFilename();
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
        // String inputFileName = argumentClass.getInputDatasetPathName();
        // String trainDataSet = argumentClass.getInputTrainDataset();
        String expriRunId = argumentClass.getExperiRunId();

        String dataSetType = checkDataType(expriRunId);

        // FIXME for different datasets!
        String trainDataSet = "";
        String inputFileName = "";
        switch (dataSetType) {
            case "TAXI":
                trainDataSet = "/resources/datasets/TAXI_sample_data_senml.csv";
                inputFileName = "/resources/datasets/inputFileForTimerSpout-TAXI.csv";
                break;
            case "SYS":
                trainDataSet = "/resources/datasets/SYS_sample_data_senml.csv";
                inputFileName = "/resources/datasets/inputFileForTimerSpout-CITY.csv";

                break;
            case "FIT":
                trainDataSet = "/resources/datasets/FIT_sample_data_senml.csv";
                inputFileName = "/resources/datasets/inputFileForTimerSpout-FIT.csv";

                break;
            default:
                throw new RuntimeException("Type not recognized");
        }

        long linesCount = countLines(inputFileName);

        /*
              Properties p_ = new Properties();
              InputStream input = new FileInputStream(taskPropFilename);
              p_.load(input);

        */

        Properties p_ = new Properties();
        try (InputStream input =
                TrainJob.class.getResourceAsStream("/resources/configs/all_tasks.properties")) {
            p_.load(input);

        } catch (IOException e) {
            e.printStackTrace();
        }

        FlinkPipelineOptions options =
                PipelineOptionsFactory.create()
                        // .withValidation()
                        .as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setParallelism(1);

        // PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        PCollection<String> inputFile = p.apply(Create.of("test"));

        PCollection<SourceEntry> timerSource =
                inputFile.apply(
                        "Timer Source",
                        ParDo.of(
                                new TimerSourceBeam(
                                        inputFileName,
                                        spoutLogFileName,
                                        argumentClass.getScalingFactor(),
                                        (linesCount - 1))));

        PCollection<DbEntry> dataFromAzureDB =
                timerSource.apply(
                        "Table Read",
                        ParDo.of(
                                new TableReadBeam(
                                        p_, spoutLogFileName, dataSetType, trainDataSet)));

        PCollection<TrainEntry> linearRegressionTrain =
                dataFromAzureDB.apply(
                        "Multi Var Linear Regression",
                        ParDo.of(new LinearRegressionBeam(p_, dataSetType)));

        PCollection<AnnotateEntry> annotatedData =
                dataFromAzureDB.apply("Annotation", ParDo.of(new AnnotateBeam(p_)));

        PCollection<TrainEntry> decisionTreeData =
                annotatedData.apply(
                        "Decision Tree Train", ParDo.of(new DecisionTreeBeam(p_, dataSetType)));

        PCollection<TrainEntry> totalTrainData =
                PCollectionList.of(linearRegressionTrain)
                        .and(decisionTreeData)
                        .apply("Merge PCollections", Flatten.pCollections());
        PCollection<BlobUploadEntry> blobUpload =
                totalTrainData.apply("Blob Write", ParDo.of(new BlobWriteBeam(p_)));

        PCollection<MqttPublishEntry> mqttPublish =
                blobUpload.apply("MQTT Publish", ParDo.of(new MqttPublishBeam(p_)));

        mqttPublish.apply("Sink", ParDo.of(new Sink(sinkLogFileName)));

        PCollection<Long> count = mqttPublish.apply("Count", Count.globally());
        count.apply(
                ParDo.of(
                        new DoFn<Long, Void>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                System.out.println("Length of PCollection: " + c.element());
                            }
                        }));

        p.run().waitUntilFinish();
    }
}
