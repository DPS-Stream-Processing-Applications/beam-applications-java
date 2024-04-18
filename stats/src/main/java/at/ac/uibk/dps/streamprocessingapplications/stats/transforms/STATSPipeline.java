package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import java.util.List;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class STATSPipeline<T> extends PTransform<PCollection<String>, PCollection<byte[]>> {
  private final TypeDescriptor<T> typeDescriptor;
  private final SerializableFunction<String, T> parser;
  private final DoFn<Iterable<T>, Double> averagingFunction;
  private final DoFn<Iterable<T>, Long> distinctCountFunction;
  private final int batchSize;
  private final DoFn<KV<String, T>, KV<String, T>> kalmanFilterFunction;
  private final DoFn<KV<String, T>, KV<String, List<Double>>> slidingLinearRegressionFunction;

  public STATSPipeline(
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<String, T> parser,
      DoFn<Iterable<T>, Double> averagingFunction,
      DoFn<Iterable<T>, Long> distinctCountFunction,
      int batchSize,
      DoFn<KV<String, T>, KV<String, T>> kalmanFilterFunction,
      DoFn<KV<String, T>, KV<String, List<Double>>> slidingLinearRegressionFunction) {
    this.typeDescriptor = typeDescriptor;
    this.parser = parser;
    this.averagingFunction = averagingFunction;
    this.distinctCountFunction = distinctCountFunction;
    this.batchSize = batchSize;
    this.kalmanFilterFunction = kalmanFilterFunction;
    this.slidingLinearRegressionFunction = slidingLinearRegressionFunction;
  }

  @Override
  public PCollection<byte[]> expand(PCollection<String> input) {

    PCollection<T> parsedObjects =
        input.apply("Parse", MapElements.into(this.typeDescriptor).via(this.parser));

    PCollection<byte[]> average =
        parsedObjects
            .apply("Average", new Average<>(this.averagingFunction, batchSize))
            .apply("Visualise", new Visualise<>(new AveragePlot(), 10));

    PCollection<byte[]> kalmanAndPredict =
        parsedObjects
            .apply(WithKeys.of(""))
            .apply("KalmanFilter", ParDo.of(this.kalmanFilterFunction))
            .apply("SlidingLinearReg", ParDo.of(this.slidingLinearRegressionFunction))
            .apply(Values.create())
            .apply("Visualise", new Visualise<>(new KalmanRegressionPlot(), 10));

    PCollection<byte[]> distinctCount =
        parsedObjects
            .apply(
                "Count Distinct", new DistinctCount<>(this.distinctCountFunction, this.batchSize))
            .apply("Visualise", new Visualise<>(new DistinctCountPlot(), 10));
    return kalmanAndPredict;
  }
}
