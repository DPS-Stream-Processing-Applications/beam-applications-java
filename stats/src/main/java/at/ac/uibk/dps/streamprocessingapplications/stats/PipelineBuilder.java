package at.ac.uibk.dps.streamprocessingapplications.stats;

import at.ac.uibk.dps.streamprocessingapplications.shared.FitSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.GridSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.TaxiSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import at.ac.uibk.dps.streamprocessingapplications.shared.sources.ReadSenMLSource;
import at.ac.uibk.dps.streamprocessingapplications.stats.taxi.AveragingFunction;
import at.ac.uibk.dps.streamprocessingapplications.stats.taxi.DistinctCountFunction;
import at.ac.uibk.dps.streamprocessingapplications.stats.taxi.KalmanGetter;
import at.ac.uibk.dps.streamprocessingapplications.stats.taxi.KalmanSetter;
import at.ac.uibk.dps.streamprocessingapplications.stats.transforms.KalmanFilterFunction;
import at.ac.uibk.dps.streamprocessingapplications.stats.transforms.STATSPipeline;
import at.ac.uibk.dps.streamprocessingapplications.stats.transforms.SlidingLinearRegression;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.TypeDescriptor;

public class PipelineBuilder {
  static Pipeline buildTAXIPipeline(FlinkPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(new ReadSenMLSource("senml-cleaned"))
        .apply(
            new STATSPipeline<>(
                TypeDescriptor.of(TaxiRide.class),
                TaxiSenMLParserJSON::parseSenMLPack,
                new AveragingFunction(),
                new DistinctCountFunction(),
                5,
                new KalmanFilterFunction<>(new KalmanGetter(), new KalmanSetter()),
                new SlidingLinearRegression<>(new KalmanGetter(), 10, 10)));

    return pipeline;
  }

  static Pipeline buildFITPipeline(FlinkPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(new ReadSenMLSource("senml-cleaned"))
        .apply(
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
                    new at.ac.uibk.dps.streamprocessingapplications.stats.fit.KalmanGetter(),
                    10,
                    10)));

    return pipeline;
  }

  static Pipeline buildGRIDPipeline(FlinkPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(new ReadSenMLSource("senml-cleaned"))
        .apply(
            new STATSPipeline<>(
                TypeDescriptor.of(GridMeasurement.class),
                GridSenMLParserJSON::parseSenMLPack,
                new at.ac.uibk.dps.streamprocessingapplications.stats.grid.AveragingFunction(),
                new at.ac.uibk.dps.streamprocessingapplications.stats.grid.DistinctCountFunction(),
                5,
                new KalmanFilterFunction<>(
                    new at.ac.uibk.dps.streamprocessingapplications.stats.grid.KalmanGetter(),
                    new at.ac.uibk.dps.streamprocessingapplications.stats.grid.KalmanSetter()),
                new SlidingLinearRegression<>(
                    new at.ac.uibk.dps.streamprocessingapplications.stats.grid.KalmanGetter(),
                    10,
                    10)));
    return pipeline;
  }
}
