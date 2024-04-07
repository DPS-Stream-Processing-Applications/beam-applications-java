package at.ac.uibk.dps.streamprocessingapplications.etl;

// spotless:off
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.InterpolationFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.RangeFilterFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.transforms.ETLPipeline;
import at.ac.uibk.dps.streamprocessingapplications.shared.TaxiSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.stream.Stream;

// spotless:on
public class FlinkJob {

  public static void main(String[] args) {
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    // options.setParallelism(4);

    Pipeline pipeline = Pipeline.create(options);


    pipeline
        .apply(Create.of(TaxiTestObjects.testPacks))
        .apply(
            new ETLPipeline<>(
                TypeDescriptor.of(TaxiRide.class),
                TaxiSenMLParserJSON::parseSenMLPack,
                new RangeFilterFunction(),
                TaxiTestObjects.buildTestBloomFilter(),
                new InterpolationFunction(),
                5))
        .apply(MapElements.into(TypeDescriptors.strings()).via(TaxiRide::toString))
        .apply(ParDo.of(new FlinkJob.PrintFn()));

    pipeline.run();
  }

  static class PrintFn extends DoFn<String, Void> {
    @ProcessElement
    public void processElement(@Element String element) {
      System.out.println(element);
    }
  }
}
