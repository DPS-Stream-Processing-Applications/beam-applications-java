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
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlinkJob {

  public static long countLines(String resourceFileName) {
    long lines = 0;
    try (InputStream inputStream = FlinkJob.class.getResourceAsStream(resourceFileName)) {
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
    throw new IllegalArgumentException("No valid datatype given");
  }

  public static void main(String[] args) {

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
    String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
    String expriRunId = argumentClass.getExperiRunId();

    String dataSetType = checkDataType(expriRunId);

    String trainDataSet;
    String inputFileName;
    switch (dataSetType) {
      case "TAXI":
        trainDataSet = "/resources/datasets/TAXI_sample_data_senml.csv";
        inputFileName = "/resources/datasets/inputFileForTimerSpout-TAXI.csv";
        break;
      case "GRID":
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

    Properties p_ = new Properties();
    try (InputStream input =
        FlinkJob.class.getResourceAsStream("/resources/configs/all_tasks.properties")) {
      p_.load(input);

    } catch (IOException e) {
      e.printStackTrace();
    }

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
    options.setJobName("predjob");

    // PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    String kafkaBootstrapServers = argumentClass.getBootStrapServerKafka();

    PCollection<String> inputFile = p.apply(new ReadSenMLSource("senml-source"));

    PCollection<SourceEntry> timerSource =
        inputFile.apply(
            "Timer Source",
            ParDo.of(
                new TimerSourceBeam(
                    inputFileName, spoutLogFileName, argumentClass.getScalingFactor())));

    PCollection<DbEntry> dataFromAzureDB =
        timerSource.apply(
            "Table Read",
            ParDo.of(new TableReadBeam(p_, spoutLogFileName, dataSetType, trainDataSet)));

    PCollection<TrainEntry> linearRegressionTrain =
        dataFromAzureDB.apply(
            "Multi Var Linear Regression", ParDo.of(new LinearRegressionBeam(p_, dataSetType)));

    PCollection<AnnotateEntry> annotatedData =
        dataFromAzureDB.apply("Annotation", ParDo.of(new AnnotateBeam(p_)));

    PCollection<TrainEntry> decisionTreeData =
        annotatedData.apply(
            "Decision Tree Train",
            ParDo.of(new DecisionTreeBeam(p_, dataSetType, databaseUrl, databaseName)));

    PCollection<TrainEntry> totalTrainData =
        PCollectionList.of(linearRegressionTrain)
            .and(decisionTreeData)
            .apply("Merge PCollections", Flatten.pCollections());
    PCollection<BlobUploadEntry> blobUpload =
        totalTrainData.apply("Blob Write", ParDo.of(new BlobWriteBeam(p_)));

    PCollection<MqttPublishEntry> mqttPublish =
        blobUpload.apply(
            "MQTT Publish",
            ParDo.of(new KafkaPublishBeam(p_, kafkaBootstrapServers, "train-sub-task")));

    mqttPublish.apply("Sink", ParDo.of(new Sink(sinkLogFileName)));
    p.run();
  }
}
