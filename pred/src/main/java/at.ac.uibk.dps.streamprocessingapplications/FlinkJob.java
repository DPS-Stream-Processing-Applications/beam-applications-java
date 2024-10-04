package at.ac.uibk.dps.streamprocessingapplications;

import at.ac.uibk.dps.streamprocessingapplications.beam.*;
import at.ac.uibk.dps.streamprocessingapplications.database.WriteToDatabase;
import at.ac.uibk.dps.streamprocessingapplications.entity.*;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentClass;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentParser;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.PredCustomOptions;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FlinkJob {

  public static String checkDataType(String expriRunId) {
    if (expriRunId.contains("SYS")) {
      return "SYS";
    } else if (expriRunId.contains("FIT")) {
      return "FIT";
    } else if (expriRunId.contains("TAXI")) {
      return "TAXI";

    } else if (expriRunId.contains("GRID")) {
      return "GRID";
    }
    throw new RuntimeException("No valid DataSetType given");
  }

  public static void main(String[] args) {
    ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
    if (argumentClass == null) {
      System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
      return;
    }

    String logFilePrefix =
        argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + ".log";

    String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
    String expriRunId = argumentClass.getExperiRunId();

    Properties p_ = new Properties();
    try (InputStream input =
        FlinkJob.class.getResourceAsStream("/resources/configs/all_tasks.properties")) {
      p_.load(input);

    } catch (IOException e) {
      e.printStackTrace();
    }

    String inputFileName;
    String dataSetType = checkDataType(expriRunId);
    switch (dataSetType) {
      case "TAXI":
        inputFileName = "/resources/datasets/TAXI_sample_data_senml.csv";
        break;
      case "SYS":

      case "GRID":
        inputFileName = "/resources/datasets/SYS_sample_data_senml.csv";
        break;
      case "FIT":
        inputFileName = "/resources/datasets/FIT_sample_data_senml.csv";
        break;

      default:
        throw new RuntimeException("Type not recognized");
    }

    String kafkaBootstrapServers = argumentClass.getBootStrapServerKafka();
    boolean isJson = inputFileName.contains("senml");
    String databaseUrl = argumentClass.getDatabaseUrl();
    String databaseName = "mydb";

    WriteToDatabase writeToDatabase = new WriteToDatabase(databaseUrl, databaseName);
    writeToDatabase.prepareDataBaseForApplication();

    PipelineOptionsFactory.register(PredCustomOptions.class);
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PredCustomOptions.class);
    options.setRunner(FlinkRunner.class);
    options.setStreaming(true);
    options.setLatencyTrackingInterval(5L);
    options.setJobName("PRED");

    Pipeline p = Pipeline.create(options);

    PCollection<String> inputFile = p.apply(new ReadSenMLSource(argumentClass.getKafkaTopic()));

    PCollection<String> inputFile2 = p.apply(new ReadSenMLSource("pred-model"));

    PCollection<SourceEntry> sourceData =
        inputFile.apply(
            "Source", ParDo.of(new SourceBeam(inputFileName, spoutLogFileName, dataSetType)));

    PCollection<MqttSubscribeEntry> sourceDataMqtt =
        inputFile2.apply("MQTT Subscribe", ParDo.of(new KafkaSubscribeBeam(p_)));
    PCollection<BlobReadEntry> blobRead =
        sourceDataMqtt.apply("Blob Read", ParDo.of(new BlobReadBeam(p_)));

    PCollection<SenMlEntry> mlParseData =
        sourceData.apply("SenML Parse", ParDo.of(new ParsePredictBeam(p_, dataSetType, isJson)));

    PCollection<LinearRegressionEntry> linearRegression1 =
        mlParseData.apply(
            "Multi Var Linear Regression",
            ParDo.of(new LinearRegressionBeam1(p_, dataSetType, databaseUrl, databaseName)));

    PCollection<LinearRegressionEntry> linearRegression2 =
        blobRead.apply(
            "Multi Var Linear Regression",
            ParDo.of(new LinearRegressionBeam2(p_, dataSetType, databaseUrl, databaseName)));

    PCollection<LinearRegressionEntry> linearRegression =
        PCollectionList.of(linearRegression1)
            .and(linearRegression2)
            .apply("Merge PCollections", Flatten.pCollections());

    PCollection<DecisionTreeEntry> decisionTree1 =
        blobRead.apply("Decision Tree", ParDo.of(new DecisionTreeBeam1(p_, dataSetType)));

    PCollection<DecisionTreeEntry> decisionTree2 =
        mlParseData.apply("Decision Tree", ParDo.of(new DecisionTreeBeam2(p_, dataSetType)));
    PCollection<DecisionTreeEntry> decisionTree =
        PCollectionList.of(decisionTree1)
            .and(decisionTree2)
            .apply("Merge PCollections", Flatten.pCollections());

    PCollection<AverageEntry> average =
        mlParseData.apply("Average", ParDo.of(new AverageBeam(p_, dataSetType)));

    PCollection<ErrorEstimateEntry> errorEstimate1 =
        linearRegression.apply("Error Estimate", ParDo.of(new ErrorEstimateBeam1(p_, dataSetType)));

    PCollection<ErrorEstimateEntry> errorEstimate2 =
        average.apply("Error Estimate", ParDo.of(new ErrorEstimateBeam2(p_, dataSetType)));
    PCollection<ErrorEstimateEntry> errorEstimate =
        PCollectionList.of(errorEstimate1)
            .and(errorEstimate2)
            .apply("Merge PCollections", Flatten.pCollections());

    PCollection<String> publish1 =
        errorEstimate.apply(
            "Format ErrorEstimateEntry to String",
            MapElements.into(TypeDescriptors.strings())
                .via((MqttPublishInput input) -> input.getMsgid()));
    PCollection<String> publish2 =
        decisionTree.apply(
            "Format ErrorEstimateEntry to String",
            MapElements.into(TypeDescriptors.strings())
                .via((MqttPublishInput input) -> input.getMsgid()));
    PCollection<String> publish =
        PCollectionList.of(publish1)
            .and(publish2)
            .apply("Merge PCollections", Flatten.pCollections());

    PCollection<Long> latencies1 =
        errorEstimate.apply(
            "Format ErrorEstimateEntry to Arrival Time",
            MapElements.into(TypeDescriptors.longs())
                .via((MqttPublishInput input) -> input.getArrivalTime()));

    PCollection<Long> latencies2 =
        decisionTree.apply(
            "Format ErrorEstimateEntry to Arrival Time",
            MapElements.into(TypeDescriptors.longs())
                .via((MqttPublishInput input) -> input.getArrivalTime()));

    PCollection<Long> latency =
        PCollectionList.of(latencies1)
            .and(latencies2)
            .apply("Merge PCollections", Flatten.pCollections());

    publish.apply(new WriteStringSink("pred-publish"));

    PCollection<Long> out = latency.apply("Sink", ParDo.of(new Sink()));
    p.run();
  }
}
