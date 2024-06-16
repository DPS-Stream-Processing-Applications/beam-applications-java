package at.ac.uibk.dps.streamprocessingapplications.etl;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class FlinkJobGRID {

  public static void main(String[] args) {
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    options.setJobName("ETL-GRID");

    PipelineBuilder.buildGRIDPipeline(options).run();

  }
}
