package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

public class Average<T, O> extends PTransform<PCollection<T>, PCollection<O>> {
  private DoFn<Iterable<T>, O> averagingFunction;
  private int batchSize;

  public Average(DoFn<Iterable<T>, O> averagingFunction, int batchSize) {
    this.averagingFunction = averagingFunction;
    this.batchSize = batchSize;
  }

  @Override
  public PCollection<O> expand(PCollection<T> input) {
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
