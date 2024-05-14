package at.ac.uibk.dps.streamprocessingapplications.etl;

import at.ac.uibk.dps.streamprocessingapplications.etl.fit.AnnotationFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.fit.InterpolationFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.fit.RangeFilterFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.transforms.ETLPipeline;
import at.ac.uibk.dps.streamprocessingapplications.shared.FitSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import at.ac.uibk.dps.streamprocessingapplications.shared.sinks.StoreStringInDBSink;
import at.ac.uibk.dps.streamprocessingapplications.shared.sinks.WriteStringSink;
import at.ac.uibk.dps.streamprocessingapplications.shared.sources.ReadSenMLSource;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FlinkJobFIT {

  public static void main(String[] args) {
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    // options.setParallelism(4);

    Pipeline pipeline = Pipeline.create(options);
    PCollection<String> etl_strings =
        pipeline
            .apply(new ReadSenMLSource("senml-source"))
            .apply(
                new ETLPipeline<>(
                    TypeDescriptor.of(FitnessMeasurements.class),
                    FitSenMLParserJSON::parseSenMLPack,
                    new RangeFilterFunction(),
                    // TaxiTestObjects.buildTestBloomFilter(),
                    null,
                    new InterpolationFunction(),
                    5,
                    new AnnotationFunction()))
            .apply(
                "Serialize SenML to String",
                MapElements.into(TypeDescriptors.strings()).via(FitnessMeasurements::toString));
    etl_strings.apply(new WriteStringSink("senml-cleaned"));
    etl_strings.apply(new StoreStringInDBSink("senml-cleaned"));

    pipeline.run();
  }
}
