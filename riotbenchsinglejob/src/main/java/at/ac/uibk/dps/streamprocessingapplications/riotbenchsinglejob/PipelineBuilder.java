package at.ac.uibk.dps.streamprocessingapplications.riotbenchsinglejob;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;

// public class PipelineBuilderETL
//  extends at.ac.uibk.dps.streamprocessingapplications.etl.PipelineBuilder {}

// public class PipelineBuilderSTATS
//    extends at.ac.uibk.dps.streamprocessingapplications.stats.PipelineBuilder {}

// public class PipelineBuilderTRAIN extends
// at.ac.uibk.dps.streamprocessingapplications.train.PipelineBuilder {}
// public class PipelineBuilderPRED extends
// at.ac.uibk.dps.streamprocessingapplications.pred.PipelineBuilder {}

public class PipelineBuilder {
  static Pipeline buildTAXIPipeline(FlinkPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    return pipeline;
  }

  static Pipeline buildFITPipeline(FlinkPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    return pipeline;
  }

  static Pipeline buildGRIDPipeline(FlinkPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    // PipelineBuilderETL pipelineBuilderETL = new PipelineBuilderETL();
    return pipeline;
  }
}
