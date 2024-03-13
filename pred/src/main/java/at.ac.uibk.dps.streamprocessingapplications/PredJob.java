package at.ac.uibk.dps.streamprocessingapplications;

import at.ac.uibk.dps.streamprocessingapplications.beam.*;
import at.ac.uibk.dps.streamprocessingapplications.entity.*;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentClass;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentParser;
import java.io.*;
import java.util.Properties;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

// TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class PredJob {

    public static long countLines(String csvFile) {
        long lines = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
            while (reader.readLine() != null) lines++;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error when counting lines in csv-file " + e);
        }
        return lines;
    }

    public static String checkDataType(String fileName) {
        if (fileName.contains("SYS")) {
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

    public static void main(String[] args) throws IOException {
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
        // String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
        String taskPropFilename = argumentClass.getTasksPropertiesFilename();
        String inputFileName = argumentClass.getInputDatasetPathName();
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;

        Properties p_ = new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);
        long lines = countLines(inputFileName);
        String dataSetType = checkDataType(inputFileName);
        if (dataSetType == null) {
            throw new RuntimeException("Type could not be detected");
        }

        FlinkPipelineOptions options =
                PipelineOptionsFactory.create()
                        // .withValidation()
                        .as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setParallelism(1);

        // Create pipeline
        Pipeline p = Pipeline.create(options);

        // PCollection<String> inputFile = p.apply(TextIO.read().from(inputFileName));
        PCollection<String> inputFile = p.apply(Create.of("test"));

        PCollection<MqttSubscribeEntry> sourceDataMqtt =
                inputFile.apply("MQTT Subscribe", ParDo.of(new MqttSubscribeBeam(p_)));

        PCollection<BlobReadEntry> blobRead =
                sourceDataMqtt.apply("Blob Read", ParDo.of(new BlobReadBeam(p_)));

        PCollection<SourceEntry> sourceData =
                inputFile.apply(
                        "Source",
                        ParDo.of(
                                new SourceBeam(
                                        inputFileName,
                                        spoutLogFileName,
                                        argumentClass.getScalingFactor(),
                                        lines)));

        PCollection<SenMlEntry> mlParseData =
                sourceData.apply("SenML Parse", ParDo.of(new ParsePredictBeam(p_, dataSetType)));

        PCollection<LinearRegressionEntry> linearRegression1 =
                mlParseData.apply(
                        "Multi Var Linear Regression",
                        ParDo.of(new LinearRegressionBeam1(p_, dataSetType)));

        PCollection<LinearRegressionEntry> linearRegression2 =
                blobRead.apply(
                        "Multi Var Linear Regression",
                        ParDo.of(new LinearRegressionBeam2(p_, dataSetType)));

        PCollection<LinearRegressionEntry> linearRegression =
                PCollectionList.of(linearRegression1)
                        .and(linearRegression2)
                        .apply("Merge PCollections", Flatten.pCollections());

        PCollection<DecisionTreeEntry> decisionTree1 =
                blobRead.apply("Decision Tree", ParDo.of(new DecisionTreeBeam1(p_, dataSetType)));

        PCollection<DecisionTreeEntry> decisionTree2 =
                mlParseData.apply(
                        "Decision Tree", ParDo.of(new DecisionTreeBeam2(p_, dataSetType)));
        PCollection<DecisionTreeEntry> decisionTree =
                PCollectionList.of(decisionTree1)
                        .and(decisionTree2)
                        .apply("Merge PCollections", Flatten.pCollections());

        PCollection<AverageEntry> average =
                mlParseData.apply("Average", ParDo.of(new AverageBeam(p_, dataSetType)));

        PCollection<ErrorEstimateEntry> errorEstimate1 =
                linearRegression.apply(
                        "Error Estimate", ParDo.of(new ErrorEstimateBeam1(p_, dataSetType)));

        PCollection<ErrorEstimateEntry> errorEstimate2 =
                average.apply("Error Estimate", ParDo.of(new ErrorEstimateBeam2(p_, dataSetType)));
        PCollection<ErrorEstimateEntry> errorEstimate =
                PCollectionList.of(errorEstimate1)
                        .and(errorEstimate2)
                        .apply("Merge PCollections", Flatten.pCollections());

        PCollection<MqttPublishEntry> publish1 =
                errorEstimate.apply("MQTT Publish", ParDo.of(new MqttPublishBeam1(p_)));
        PCollection<MqttPublishEntry> publish2 =
                decisionTree.apply("MQTT Publish", ParDo.of(new MqttPublishBeam2(p_)));
        PCollection<MqttPublishEntry> publish =
                PCollectionList.of(publish1)
                        .and(publish2)
                        .apply("Merge PCollections", Flatten.pCollections());

        PCollection<String> out = publish.apply("Sink", ParDo.of(new Sink(sinkLogFileName)));

        p.run();
    }
}
