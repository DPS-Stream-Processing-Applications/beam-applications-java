package at.ac.uibk.dps.streamprocessingapplications.etl;

import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.AnnotationFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.InterpolationFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.RangeFilterFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.transforms.ETLPipeline;
import at.ac.uibk.dps.streamprocessingapplications.shared.TaxiSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
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

public class FlinkJobTAXI {

  public static void main(String[] args) {
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    options.setJobName("ETL-TAXI");

    PipelineBuilder.buildTAXIPipeline(options).run();
  }
}
