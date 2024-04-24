package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

public class Average<T, Double> extends PTransform<PCollection<T>, PCollection<Double>> {
  private DoFn<Iterable<T>, Double> averagingFunction;
  private int batchSize;

  public Average(DoFn<Iterable<T>, Double> averagingFunction, int batchSize) {
    this.averagingFunction = averagingFunction;
    this.batchSize = batchSize;
  }

  @Override
  public PCollection<Double> expand(PCollection<T> input) {
    /* INFO:
     * `GroupIntoBatches` only supports grouping for key-value pairs.
     * Therefore, a pseudo mapping to the same key is performed.
     */
    return input
        .apply(WithKeys.of(""))
        .apply(GroupIntoBatches.ofSize(this.batchSize))
        .apply(Values.create())
        .apply(ParDo.of(this.averagingFunction));
  }
}
