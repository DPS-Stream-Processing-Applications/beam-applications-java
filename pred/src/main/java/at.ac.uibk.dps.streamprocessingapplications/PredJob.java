package at.ac.uibk.dps.streamprocessingapplications;

import at.ac.uibk.dps.streamprocessingapplications.beam.*;
import at.ac.uibk.dps.streamprocessingapplications.database.WriteToDatabase;
import at.ac.uibk.dps.streamprocessingapplications.entity.*;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentClass;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.ArgumentParser;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.PredCustomOptions;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class PredJob {

  public static long countLines(String resourceFileName) {
    long lines = 0;
    try (InputStream inputStream = PredJob.class.getResourceAsStream(resourceFileName)) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
        while ((reader.readLine()) != null) {
          lines++;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Error when counting lines in resource file: " + e.getMessage());
    }
    return lines;
  }

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
        PredJob.class.getResourceAsStream("/resources/configs/all_tasks.properties")) {
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
        inputFileName = "/resources/datasets/SYS_sample_data_senml.csv";

        break;
      case "FIT":
        inputFileName = "/resources/datasets/FIT_sample_data_senml.csv";

        break;
      default:
        throw new RuntimeException("Type not recognized");
    }

    long lines = countLines(inputFileName);
    String kafkaBootstrapServers = argumentClass.getBootStrapServerKafka();
    String kafkaTopic = argumentClass.getKafkaTopic();
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

    Pipeline p = Pipeline.create(options);

    PCollection<String> inputFile = p.apply(Create.of("test"));

    PCollection<SourceEntry> sourceData =
        inputFile.apply(
            "Source",
            ParDo.of(
                new SourceBeam(
                    inputFileName,
                    spoutLogFileName,
                    lines,
                    kafkaBootstrapServers,
                    kafkaTopic,
                    dataSetType)));

    PCollection<MqttSubscribeEntry> sourceDataMqtt =
        inputFile.apply(
            "MQTT Subscribe",
            ParDo.of(new KafkaSubscribeBeam(p_, kafkaBootstrapServers, "pred-sub-task")));
    PCollection<BlobReadEntry> blobRead =
        sourceDataMqtt.apply("Blob Read", ParDo.of(new BlobReadBeam(p_)));

    PCollection<SenMlEntry> mlParseData =
        sourceData.apply(
            "SenML Parse",
            ParDo.of(new ParsePredictBeam(p_, dataSetType, isJson, databaseUrl, databaseName)));

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

    PCollection<MqttPublishEntry> publish1 =
        errorEstimate.apply(
            "MQTT Publish",
            ParDo.of(new KafkaPublishBeam1(p_, kafkaBootstrapServers, "pred-sub-task")));

    PCollection<MqttPublishEntry> publish2 =
        decisionTree.apply(
            "MQTT Publish",
            ParDo.of(new KafkaPublishBeam2(p_, kafkaBootstrapServers, "pred-sub-task")));
    PCollection<MqttPublishEntry> publish =
        PCollectionList.of(publish1)
            .and(publish2)
            .apply("Merge PCollections", Flatten.pCollections());

    PCollection<String> out = publish.apply("Sink", ParDo.of(new Sink()));
    p.run();
  }
}
