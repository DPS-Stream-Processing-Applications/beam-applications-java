package at.ac.uibk.dps.streamprocessingapplications.etl.transforms;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class Interpolate<T> extends PTransform<PCollection<T>, PCollection<T>> {
  private final TypeDescriptor<T> type;
  private final SerializableFunction<Iterable<T>, Iterable<T>> interpolationFunction;
  private final int batchSize;

  Interpolate(
      TypeDescriptor<T> type,
      SerializableFunction<Iterable<T>, Iterable<T>> interpolationFunction,
      int batchSize) {
    this.type = type;
    this.interpolationFunction = interpolationFunction;
    this.batchSize = batchSize;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    /* INFO:
     * `GroupIntoBatches` only supports grouping for key-value pairs.
     * Therefore, a pseudo mapping to the same key is performed.
     */
    return input
        .apply(WithKeys.of(""))
        .apply(GroupIntoBatches.ofSize(this.batchSize))
        .apply(Values.create())
        .apply(FlatMapElements.into(type).via(this.interpolationFunction::apply));
  }
}
