package at.ac.uibk.dps.streamprocessingapplications;

import at.ac.uibk.dps.streamprocessingapplications.beam.*;
import at.ac.uibk.dps.streamprocessingapplications.entity.*;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentClass;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentParser;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.io.*;
import java.util.Properties;

public class TrainJob {

    public static long countLines(String csvFile) {
        long lines = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
            while (reader.readLine() != null) lines++;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error when counting lines in csv-file:" + e);
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
        String inputFileName = argumentClass.getInputDatasetPathName();
        String trainDataSet = argumentClass.getInputTrainDataset();

        long linesCount = countLines(inputFileName);
        String dataSetType = checkDataType(inputFileName);

        Properties p_ = new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);

        FlinkPipelineOptions options =
                PipelineOptionsFactory.create()
                        // .withValidation()
                        .as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setParallelism(1);

        // PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        PCollection<String> inputFile = p.apply(TextIO.read().from(inputFileName));
        inputFile = p.apply(Create.of("test"));
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
        dataFromAzureDB.apply(
                ParDo.of(
                        new DoFn<DbEntry, Void>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                System.out.println("Db: " + c.element()); // Log the element
                            }
                        }));
        dataFromAzureDB.apply(
                ParDo.of(
                        new DoFn<DbEntry, Void>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                System.out.println(c.element());
                            }
                        }));

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
