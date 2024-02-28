package at.ac.uibk.dps.streamprocessingapplications.etl;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import at.ac.uibk.dps.streamprocessingapplications.etl.transforms.GroupSenMLRecordsByFullName;
import at.ac.uibk.dps.streamprocessingapplications.etl.model.SenMLRecord;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class EtlJob {

  public static void main(String[] args) {
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    // options.setParallelism(4);

    List<String> sensorValues =
        Arrays.asList(
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 25.3, \"t\": 1645467200}",
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 25.7, \"t\": 1645467500}",
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 26.2, \"t\": 1645467800}",
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 26.8, \"t\": 1645468100}",
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 27.1, \"t\": 1645468400}",
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 27.5, \"t\": 1645468700}",
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 27.9, \"t\": 1645469000}",
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 28.3, \"t\": 1645469300}",
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 28.7, \"t\": 1645469600}",
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 29.2, \"t\": 1645469900}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 50, \"t\": 1645467200}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 51, \"t\": 1645467500}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 52, \"t\": 1645467800}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 53, \"t\": 1645468100}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 54, \"t\": 1645468400}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 55, \"t\": 1645468700}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 56, \"t\": 1645469000}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 57, \"t\": 1645469300}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 58, \"t\": 1645469600}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 59, \"t\": 1645469900}");

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read data", Create.of(sensorValues))
        .apply(
            "Parse SenMLRecord POJO",
            MapElements.into(TypeDescriptor.of(SenMLRecord.class)).via(SenMLRecord::new))
        .apply("Group records by full name", new GroupSenMLRecordsByFullName())
        .apply(
            "Get timestamp",
            MapElements.into(TypeDescriptors.iterables(TypeDescriptor.of(Instant.class)))
                .via(
                    records ->
                        StreamSupport.stream(records.spliterator(), true)
                            .map(SenMLRecord::getTime)
                            .collect(Collectors.toList())))
        .apply(ToString.elements())
        .apply(ParDo.of(new PrintFn()));

    pipeline.run();
  }

  static class PrintFn extends DoFn<String, Void> {
    @ProcessElement
    public void processElement(@Element String element) {
      System.out.println(element);
    }
  }
}
