package at.ac.uibk.dps.streamprocessingapplications.etl;

import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.AnnotationFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.InterpolationFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.RangeFilterFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.transforms.ETLPipeline;
import at.ac.uibk.dps.streamprocessingapplications.shared.TaxiSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import at.ac.uibk.dps.streamprocessingapplications.shared.sinks.WriteStringSink;
import at.ac.uibk.dps.streamprocessingapplications.shared.sources.ReadSenMLSource;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FlinkJob {

  public static void main(String[] args) {
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    // options.setParallelism(4);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(new ReadSenMLSource("senml-source"))
        .apply(
            new ETLPipeline<>(
                TypeDescriptor.of(TaxiRide.class),
                TaxiSenMLParserJSON::parseSenMLPack,
                new RangeFilterFunction(),
                // TaxiTestObjects.buildTestBloomFilter(),
                null,
                new InterpolationFunction(),
                5,
                new AnnotationFunction()))
        .apply(
            "Serialize SenML to String",
            MapElements.into(TypeDescriptors.strings()).via(TaxiRide::toString))
        // .apply(WithKeys.of())
        .apply(new WriteStringSink("senml-cleaned"));

    // .apply(ParDo.of(new FlinkJob.PrintFn()));

    pipeline.run();
  }

  static class PrintFn extends DoFn<String, Void> {
    @ProcessElement
    public void processElement(@Element String element) {
      System.out.println(element);
    }
  }
}
