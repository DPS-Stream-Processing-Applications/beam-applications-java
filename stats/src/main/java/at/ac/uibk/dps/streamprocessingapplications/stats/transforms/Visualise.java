package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

public class Visualise<T> extends PTransform<PCollection<T>, PCollection<byte[]>> {

  private DoFn<Iterable<TimestampedValue<T>>, byte[]> plottingFunction;
  private int batchSize;

  public Visualise(DoFn<Iterable<TimestampedValue<T>>, byte[]> plottingFunction, int batchSize) {
    this.plottingFunction = plottingFunction;
    this.batchSize = batchSize;
  }

  @Override
  public PCollection<byte[]> expand(PCollection<T> input) {
    /* INFO:
     * `GroupIntoBatches` only supports grouping for key-value pairs.
     * Therefore, a pseudo mapping to the same key is performed.
     */
    return input
        /* NOTE:
         * Adding timestamps of when the objects arrived at this transform.
         * Some transforms like `DistinctCount` create new values which will not have a `proper` timestamp.
         */
        .apply(
            // NOTE: Using a `DoFn` instead of `MapElement` because of generic type.
            new AddTimestamp<>())
        .apply(WithKeys.of(""))
        .apply(GroupIntoBatches.ofSize(this.batchSize))
        .apply(Values.create())
        .apply(ParDo.of(this.plottingFunction));
  }
}
