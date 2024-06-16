package at.ac.uibk.dps.streamprocessingapplications.etl;

import at.ac.uibk.dps.streamprocessingapplications.etl.grid.AnnotationFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.grid.InterpolationFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.grid.RangeFilterFunction;
import at.ac.uibk.dps.streamprocessingapplications.etl.transforms.ETLPipeline;
import at.ac.uibk.dps.streamprocessingapplications.shared.GridSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import at.ac.uibk.dps.streamprocessingapplications.shared.sinks.StoreStringInDBSink;
import at.ac.uibk.dps.streamprocessingapplications.shared.sinks.WriteStringSink;
import at.ac.uibk.dps.streamprocessingapplications.shared.sources.ReadSenMLSource;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FlinkJobGRID {

  public static void main(String[] args) {
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    options.setJobName("ETL-GRID");

    PipelineBuilder.buildGRIDPipeline(options);
  }
}
