package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

public class DistinctCount<T> extends PTransform<PCollection<T>, PCollection<Long>> {
  private DoFn<Iterable<T>, Long> distinctCountFunction;
  private int batchSize;

  public DistinctCount(DoFn<Iterable<T>, Long> distinctCountFunction, int batchSize) {
    this.distinctCountFunction = distinctCountFunction;
    this.batchSize = batchSize;
  }

  @Override
  public PCollection<Long> expand(PCollection<T> input) {
    /* INFO:
     * `GroupIntoBatches` only supports grouping for key-value pairs.
     * Therefore, a pseudo mapping to the same key is performed.
     */
    return input
        .apply(WithKeys.of(""))
        .apply(GroupIntoBatches.ofSize(this.batchSize))
        .apply(Values.create())
        .apply(ParDo.of(this.distinctCountFunction));
  }
}
