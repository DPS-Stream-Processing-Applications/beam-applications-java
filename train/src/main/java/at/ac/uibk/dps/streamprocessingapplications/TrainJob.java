package at.ac.uibk.dps.streamprocessingapplications;

import at.ac.uibk.dps.streamprocessingapplications.beam.*;
import at.ac.uibk.dps.streamprocessingapplications.entity.*;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentClass;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentParser;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class TrainJob {
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

        Properties p_ = new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);

        FlinkPipelineOptions options =
                PipelineOptionsFactory.create()
                        // .withValidation()
                        .as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setParallelism(3);

        // PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        PCollection<String> inputFile = p.apply(TextIO.read().from(inputFileName));

        PCollection<SourceEntry> timerSource =
                inputFile.apply(
                        "Timer Source",
                        ParDo.of(
                                new TimerSourceBeam(
                                        inputFileName,
                                        spoutLogFileName,
                                        argumentClass.getScalingFactor())));
        PCollection<DbEntry> dataFromAzureDB =
                timerSource.apply("Table Read", ParDo.of(new TableReadBeam(p_)));

        PCollection<TrainEntry> linearRegressionTrain =
                dataFromAzureDB.apply(
                        "Multi Var Linear Regression", ParDo.of(new LinearRegressionBeam(p_)));

        PCollection<AnnotateEntry> annotatedData =
                dataFromAzureDB.apply("Annotation", ParDo.of(new AnnotateBeam(p_)));
        PCollection<TrainEntry> decisionTreeData =
                annotatedData.apply("Decision Tree Train", ParDo.of(new DecisionTreeBeam(p_)));

        PCollection<TrainEntry> totalTrainData =
                PCollectionList.of(linearRegressionTrain)
                        .and(decisionTreeData)
                        .apply("Merge PCollections", Flatten.<TrainEntry>pCollections());
        PCollection<BlobUploadEntry> blobUpload =
                totalTrainData.apply("Blob Write", ParDo.of(new BlobWriteBeam(p_)));
        PCollection<MqttPublishEntry> mqttPublish =
                blobUpload.apply("MQTT Publish", ParDo.of(new MqttPublishBeam(p_)));

        mqttPublish.apply("Sink", ParDo.of(new Sink(sinkLogFileName)));

        mqttPublish.apply(
                "Print Result",
                ParDo.of(
                        new DoFn<MqttPublishEntry, Void>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                System.out.println(c.element());
                            }
                        }));

        p.run();
    }
}
