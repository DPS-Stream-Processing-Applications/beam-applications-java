package at.ac.uibk.dps.streamprocessingapplications.riotbenchsinglejob;

import at.ac.uibk.dps.streamprocessingapplications.etl.transforms.ETLPipeline;
import at.ac.uibk.dps.streamprocessingapplications.pred.beam.*;
import at.ac.uibk.dps.streamprocessingapplications.pred.entity.*;
import at.ac.uibk.dps.streamprocessingapplications.shared.FitSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.TaxiSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import at.ac.uibk.dps.streamprocessingapplications.shared.sinks.StoreStringInDBSink;
import at.ac.uibk.dps.streamprocessingapplications.shared.sinks.WriteStringSink;
import at.ac.uibk.dps.streamprocessingapplications.shared.sources.ReadSenMLSource;
import at.ac.uibk.dps.streamprocessingapplications.stats.transforms.KalmanFilterFunction;
import at.ac.uibk.dps.streamprocessingapplications.stats.transforms.STATSPipeline;
import at.ac.uibk.dps.streamprocessingapplications.stats.transforms.SlidingLinearRegression;
import at.ac.uibk.dps.streamprocessingapplications.train.FlinkJob;
import at.ac.uibk.dps.streamprocessingapplications.train.beam.*;
import at.ac.uibk.dps.streamprocessingapplications.train.beam.KafkaPublishBeam;
import at.ac.uibk.dps.streamprocessingapplications.train.beam.Sink;
import at.ac.uibk.dps.streamprocessingapplications.train.database.WriteToDatabase;
import at.ac.uibk.dps.streamprocessingapplications.train.entity.*;
import at.ac.uibk.dps.streamprocessingapplications.train.entity.MqttPublishEntry;
import at.ac.uibk.dps.streamprocessingapplications.train.entity.SourceEntry;
import at.ac.uibk.dps.streamprocessingapplications.train.genevents.factory.PredCustomOptions;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PipelineBuilder {
  static Pipeline buildTAXIPipeline(PredCustomOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    /* ETL Pipeline */
    PCollection<String> etl_senml_cleaned =
        pipeline
            .apply(new ReadSenMLSource("senml-source"))
            .apply(
                new ETLPipeline<>(
                    TypeDescriptor.of(TaxiRide.class),
                    TaxiSenMLParserJSON::parseSenMLPack,
                    new at.ac.uibk.dps.streamprocessingapplications.etl.taxi.RangeFilterFunction(),
                    // TaxiTestObjects.buildTestBloomFilter(),
                    null,
                    new at.ac.uibk.dps.streamprocessingapplications.etl.taxi
                        .InterpolationFunction(),
                    5,
                    new at.ac.uibk.dps.streamprocessingapplications.etl.taxi.AnnotationFunction()))
            .apply(
                "Serialize SenML to String",
                MapElements.into(TypeDescriptors.strings()).via(TaxiRide::toString));
    etl_senml_cleaned.apply(new WriteStringSink("senml-cleaned"));
    etl_senml_cleaned.apply(new StoreStringInDBSink("senml-cleaned"));

    /* STATS Pipeline */
    etl_senml_cleaned.apply(
        new STATSPipeline<>(
            TypeDescriptor.of(TaxiRide.class),
            TaxiSenMLParserJSON::parseSenMLPack,
            new at.ac.uibk.dps.streamprocessingapplications.stats.taxi.AveragingFunction(),
            new at.ac.uibk.dps.streamprocessingapplications.stats.taxi.DistinctCountFunction(),
            5,
            new KalmanFilterFunction<>(
                new at.ac.uibk.dps.streamprocessingapplications.stats.taxi.KalmanGetter(),
                new at.ac.uibk.dps.streamprocessingapplications.stats.taxi.KalmanSetter()),
            new SlidingLinearRegression<>(
                new at.ac.uibk.dps.streamprocessingapplications.stats.taxi.KalmanGetter(),
                10,
                10)));

    /* TRAIN Pipeline */

    String logFilePrefix =
        "IdentityTopology"
            + "-"
            + "TRAIN"
            + "-"
            // + argumentClass.getScalingFactor()
            + ".log";
    String sinkLogFileName = "logs" + "/sink-" + logFilePrefix;
    String spoutLogFileName = "logs" + "/spout-" + logFilePrefix;
    String dataSetType = "TAXI";

    String trainDataSet = "/resources/datasets/TAXI_sample_data_senml.csv";
    String inputFileName = "/resources/datasets/inputFileForTimerSpout-TAXI.csv";

    Properties p_ = new Properties();
    try (InputStream input =
        FlinkJob.class.getResourceAsStream("/resources/configs/all_tasks.properties")) {
      p_.load(input);

    } catch (IOException e) {
      e.printStackTrace();
    }

    String databaseUrl = options.getDatabaseUrl();
    String databaseName = "mydb";

    WriteToDatabase writeToDatabase = new WriteToDatabase(databaseUrl, databaseName);
    writeToDatabase.prepareDataBaseForApplication();

    String kafkaBootstrapServers = "kafka-cluster-kafka-bootstrap:9092";

    PCollection<String> inputFile =
        pipeline.apply(
            new at.ac.uibk.dps.streamprocessingapplications.train.beam.ReadSenMLSource(
                "train-source"));

    PCollection<SourceEntry> timerSource =
        inputFile.apply(
            "Timer Source", ParDo.of(new TimerSourceBeam(inputFileName, spoutLogFileName, 60)));

    PCollection<DbEntry> dataFromAzureDB =
        timerSource.apply(
            "Table Read",
            ParDo.of(new TableReadBeam(p_, spoutLogFileName, dataSetType, trainDataSet)));

    PCollection<TrainEntry> linearRegressionTrain =
        dataFromAzureDB.apply(
            "Multi Var Linear Regression",
            ParDo.of(new LinearRegressionBeam(p_, dataSetType, databaseUrl)));

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
            ParDo.of(new KafkaPublishBeam(p_, kafkaBootstrapServers, "train-publish")));

    mqttPublish.apply("Sink", ParDo.of(new Sink(sinkLogFileName)));

    /* PRED Pipeline */

    PCollection<String> predInputFile =
        pipeline.apply(
            new at.ac.uibk.dps.streamprocessingapplications.pred.beam.ReadSenMLSource(
                "senml-cleaned"));

    PCollection<String> predInputFile2 =
        pipeline.apply(
            new at.ac.uibk.dps.streamprocessingapplications.pred.beam.ReadSenMLSource(
                "pred-model"));

    PCollection<at.ac.uibk.dps.streamprocessingapplications.pred.entity.SourceEntry> sourceData =
        predInputFile.apply(
            "Source", ParDo.of(new SourceBeam(inputFileName, spoutLogFileName, dataSetType)));

    PCollection<MqttSubscribeEntry> sourceDataMqtt =
        predInputFile2.apply("MQTT Subscribe", ParDo.of(new KafkaSubscribeBeam(p_)));
    PCollection<BlobReadEntry> blobRead =
        sourceDataMqtt.apply("Blob Read", ParDo.of(new BlobReadBeam(p_)));

    PCollection<SenMlEntry> mlParseData =
        sourceData.apply("SenML Parse", ParDo.of(new ParsePredictBeam(p_, dataSetType, true)));

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

    PCollection<at.ac.uibk.dps.streamprocessingapplications.pred.entity.MqttPublishEntry> publish1 =
        errorEstimate.apply(
            "MQTT Publish",
            ParDo.of(
                new at.ac.uibk.dps.streamprocessingapplications.pred.beam.KafkaPublishBeam(
                    p_, kafkaBootstrapServers, "pred-publish")));

    PCollection<at.ac.uibk.dps.streamprocessingapplications.pred.entity.MqttPublishEntry> publish2 =
        decisionTree.apply(
            "MQTT Publish",
            ParDo.of(
                new at.ac.uibk.dps.streamprocessingapplications.pred.beam.KafkaPublishBeam(
                    p_, kafkaBootstrapServers, "pred-publish")));
    PCollection<at.ac.uibk.dps.streamprocessingapplications.pred.entity.MqttPublishEntry> publish =
        PCollectionList.of(publish1)
            .and(publish2)
            .apply("Merge PCollections", Flatten.pCollections());

    publish.apply(
        "Sink", ParDo.of(new at.ac.uibk.dps.streamprocessingapplications.pred.beam.Sink()));

    return pipeline;
  }

  static Pipeline buildFITPipeline(PredCustomOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    /* ETL Pipeline */
    PCollection<String> etl_senml_cleaned =
        pipeline
            .apply(new ReadSenMLSource("senml-source"))
            .apply(
                new ETLPipeline<>(
                    TypeDescriptor.of(FitnessMeasurements.class),
                    FitSenMLParserJSON::parseSenMLPack,
                    new at.ac.uibk.dps.streamprocessingapplications.etl.fit.RangeFilterFunction(),
                    // TaxiTestObjects.buildTestBloomFilter(),
                    null,
                    new at.ac.uibk.dps.streamprocessingapplications.etl.fit.InterpolationFunction(),
                    5,
                    new at.ac.uibk.dps.streamprocessingapplications.etl.fit.AnnotationFunction()))
            .apply(
                "Serialize SenML to String",
                MapElements.into(TypeDescriptors.strings()).via(FitnessMeasurements::toString));
    etl_senml_cleaned.apply(new WriteStringSink("senml-cleaned"));
    etl_senml_cleaned.apply(new StoreStringInDBSink("senml-cleaned"));

    /* STATS Pipeline */
    etl_senml_cleaned.apply(
        new STATSPipeline<>(
            TypeDescriptor.of(FitnessMeasurements.class),
            FitSenMLParserJSON::parseSenMLPack,
            new at.ac.uibk.dps.streamprocessingapplications.stats.fit.AveragingFunction(),
            new at.ac.uibk.dps.streamprocessingapplications.stats.fit.DistinctCountFunction(),
            5,
            new KalmanFilterFunction<>(
                new at.ac.uibk.dps.streamprocessingapplications.stats.fit.KalmanGetter(),
                new at.ac.uibk.dps.streamprocessingapplications.stats.fit.KalmanSetter()),
            new SlidingLinearRegression<>(
                new at.ac.uibk.dps.streamprocessingapplications.stats.fit.KalmanGetter(), 10, 10)));

    /* TRAIN Pipeline */

    String logFilePrefix =
        "IdentityTopology"
            + "-"
            + "TRAIN"
            + "-"
            // + argumentClass.getScalingFactor()
            + ".log";
    String sinkLogFileName = "logs" + "/sink-" + logFilePrefix;
    String spoutLogFileName = "logs" + "/spout-" + logFilePrefix;
    String dataSetType = "FIT";

    String trainDataSet = "/resources/datasets/FIT_sample_data_senml.csv";
    String inputFileName = "/resources/datasets/inputFileForTimerSpout-FIT.csv";

    Properties p_ = new Properties();
    try (InputStream input =
        FlinkJob.class.getResourceAsStream("/resources/configs/all_tasks.properties")) {
      p_.load(input);

    } catch (IOException e) {
      e.printStackTrace();
    }

    String databaseUrl = options.getDatabaseUrl();
    String databaseName = "mydb";

    WriteToDatabase writeToDatabase = new WriteToDatabase(databaseUrl, databaseName);
    writeToDatabase.prepareDataBaseForApplication();

    String kafkaBootstrapServers = "kafka-cluster-kafka-bootstrap:9092";

    PCollection<String> inputFile =
        pipeline.apply(
            new at.ac.uibk.dps.streamprocessingapplications.train.beam.ReadSenMLSource(
                "train-source"));

    PCollection<SourceEntry> timerSource =
        inputFile.apply(
            "Timer Source", ParDo.of(new TimerSourceBeam(inputFileName, spoutLogFileName, 60)));

    PCollection<DbEntry> dataFromAzureDB =
        timerSource.apply(
            "Table Read",
            ParDo.of(new TableReadBeam(p_, spoutLogFileName, dataSetType, trainDataSet)));

    PCollection<TrainEntry> linearRegressionTrain =
        dataFromAzureDB.apply(
            "Multi Var Linear Regression",
            ParDo.of(new LinearRegressionBeam(p_, dataSetType, databaseUrl)));

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
            ParDo.of(new KafkaPublishBeam(p_, kafkaBootstrapServers, "train-publish")));

    mqttPublish.apply("Sink", ParDo.of(new Sink(sinkLogFileName)));

    /* PRED Pipeline */

    PCollection<String> predInputFile =
        pipeline.apply(
            new at.ac.uibk.dps.streamprocessingapplications.pred.beam.ReadSenMLSource(
                "senml-cleaned"));

    PCollection<String> predInputFile2 =
        pipeline.apply(
            new at.ac.uibk.dps.streamprocessingapplications.pred.beam.ReadSenMLSource(
                "pred-model"));

    PCollection<at.ac.uibk.dps.streamprocessingapplications.pred.entity.SourceEntry> sourceData =
        predInputFile.apply(
            "Source", ParDo.of(new SourceBeam(inputFileName, spoutLogFileName, dataSetType)));

    PCollection<MqttSubscribeEntry> sourceDataMqtt =
        predInputFile2.apply("MQTT Subscribe", ParDo.of(new KafkaSubscribeBeam(p_)));
    PCollection<BlobReadEntry> blobRead =
        sourceDataMqtt.apply("Blob Read", ParDo.of(new BlobReadBeam(p_)));

    PCollection<SenMlEntry> mlParseData =
        sourceData.apply("SenML Parse", ParDo.of(new ParsePredictBeam(p_, dataSetType, true)));

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

    PCollection<at.ac.uibk.dps.streamprocessingapplications.pred.entity.MqttPublishEntry> publish1 =
        errorEstimate.apply(
            "MQTT Publish",
            ParDo.of(
                new at.ac.uibk.dps.streamprocessingapplications.pred.beam.KafkaPublishBeam(
                    p_, kafkaBootstrapServers, "pred-publish")));

    PCollection<at.ac.uibk.dps.streamprocessingapplications.pred.entity.MqttPublishEntry> publish2 =
        decisionTree.apply(
            "MQTT Publish",
            ParDo.of(
                new at.ac.uibk.dps.streamprocessingapplications.pred.beam.KafkaPublishBeam(
                    p_, kafkaBootstrapServers, "pred-publish")));
    PCollection<at.ac.uibk.dps.streamprocessingapplications.pred.entity.MqttPublishEntry> publish =
        PCollectionList.of(publish1)
            .and(publish2)
            .apply("Merge PCollections", Flatten.pCollections());

    publish.apply(
        "Sink", ParDo.of(new at.ac.uibk.dps.streamprocessingapplications.pred.beam.Sink()));

    return pipeline;
  }
}
