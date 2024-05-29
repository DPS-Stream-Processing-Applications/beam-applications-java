package at.ac.uibk.dps.streamprocessingapplications.stats;

import at.ac.uibk.dps.streamprocessingapplications.shared.FitSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import at.ac.uibk.dps.streamprocessingapplications.shared.sources.ReadSenMLSource;
import at.ac.uibk.dps.streamprocessingapplications.stats.fit.AveragingFunction;
import at.ac.uibk.dps.streamprocessingapplications.stats.fit.DistinctCountFunction;
import at.ac.uibk.dps.streamprocessingapplications.stats.fit.KalmanGetter;
import at.ac.uibk.dps.streamprocessingapplications.stats.fit.KalmanSetter;
import at.ac.uibk.dps.streamprocessingapplications.stats.transforms.KalmanFilterFunction;
import at.ac.uibk.dps.streamprocessingapplications.stats.transforms.STATSPipeline;
import at.ac.uibk.dps.streamprocessingapplications.stats.transforms.SlidingLinearRegression;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.TypeDescriptor;

public class FlinkJobFIT {

  public static void main(String[] args) {
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    // options.setParallelism(4);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(new ReadSenMLSource("senml-cleaned"))
        .apply(
            new STATSPipeline<>(
                TypeDescriptor.of(FitnessMeasurements.class),
                FitSenMLParserJSON::parseSenMLPack,
                new AveragingFunction(),
                new DistinctCountFunction(),
                5,
                new KalmanFilterFunction<>(new KalmanGetter(), new KalmanSetter()),
                new SlidingLinearRegression<>(new KalmanGetter(), 10, 10)));

    pipeline.run();
  }
}
