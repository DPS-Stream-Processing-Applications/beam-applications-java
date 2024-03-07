package at.ac.uibk.dps.streamprocessingapplications;

import at.ac.uibk.dps.streamprocessingapplications.beam.*;
import at.ac.uibk.dps.streamprocessingapplications.entity.*;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentClass;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentParser;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

// TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class PredJob {
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
        System.out.println("taskPropFilename-" + taskPropFilename);
        String inputFileName = argumentClass.getInputDatasetPathName();
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;

        Properties p_ = new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);

        FlinkPipelineOptions options =
                PipelineOptionsFactory.create()
                        // .withValidation()
                        .as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setParallelism(1);

        // Create pipeline
        Pipeline p = Pipeline.create(options);

        PCollection<String> inputFile = p.apply(TextIO.read().from(inputFileName));
        inputFile = p.apply(Create.of("test"));

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
                                        argumentClass.getScalingFactor())));

        sourceData.apply(
                "Print Result",
                ParDo.of(
                        new DoFn<SourceEntry, Void>() {
                            @DoFn.ProcessElement
                            public void processElement(ProcessContext c) {
                                System.out.println(c.element());
                            }
                        }));
        /*
        PCollection<SenMlEntry> mlParseData =
                sourceData.apply("SenML Parse", ParDo.of(new ParsePredictBeam(p_)));

        PCollection<LinearRegressionEntry> linearRegression1 =
                mlParseData.apply(
                        "Multi Var Linear Regression", ParDo.of(new LinearRegressionBeam1(p_)));
        PCollection<LinearRegressionEntry> linearRegression2 =
                blobRead.apply(
                        "Multi Var Linear Regression", ParDo.of(new LinearRegressionBeam2(p_)));
        PCollection<LinearRegressionEntry> linearRegression =
                PCollectionList.of(linearRegression1)
                        .and(linearRegression2)
                        .apply("Merge PCollections", Flatten.<LinearRegressionEntry>pCollections());

        PCollection<DecisionTreeEntry> decisionTree1 =
                blobRead.apply("Decision Tree", ParDo.of(new DecisionTreeBeam1(p_)));
        PCollection<DecisionTreeEntry> decisionTree2 =
                mlParseData.apply("Decision Tree", ParDo.of(new DecisionTreeBeam2(p_)));
        PCollection<DecisionTreeEntry> decisionTree =
                PCollectionList.of(decisionTree1)
                        .and(decisionTree2)
                        .apply("Merge PCollections", Flatten.<DecisionTreeEntry>pCollections());

        PCollection<AverageEntry> average =
                mlParseData.apply("Average", ParDo.of(new AverageBeam(p_)));

        PCollection<ErrorEstimateEntry> errorEstimate1 =
                linearRegression.apply("Error Estimate", ParDo.of(new ErrorEstimateBeam1(p_)));
        PCollection<ErrorEstimateEntry> errorEstimate2 =
                average.apply("Error Estimate", ParDo.of(new ErrorEstimateBeam2(p_)));
        PCollection<ErrorEstimateEntry> errorEstimate =
                PCollectionList.of(errorEstimate1)
                        .and(errorEstimate2)
                        .apply("Merge PCollections", Flatten.<ErrorEstimateEntry>pCollections());

        PCollection<MqttPublishEntry> publish1 =
                errorEstimate.apply("MQTT Publish", ParDo.of(new MqttPublishBeam1(p_)));
        PCollection<MqttPublishEntry> publish2 =
                decisionTree.apply("MQTT Publish", ParDo.of(new MqttPublishBeam2(p_)));
        PCollection<MqttPublishEntry> publish =
                PCollectionList.of(publish1)
                        .and(publish2)
                        .apply("Merge PCollections", Flatten.<MqttPublishEntry>pCollections());

        PCollection<String> out = publish.apply("Sink", ParDo.of(new Sink(sinkLogFileName)));

        /*
        out.apply("Print Result", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));


        out.apply(
                "Print Result",
                ParDo.of(
                        new DoFn<String, Void>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                System.out.println("Print " + c.element());
                            }
                        }));

         */

        // out.apply("Write to File", TextIO.write().to("build/output/output.txt"));

        p.run().waitUntilFinish();
    }
}
