package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class STATSPipeline<T, A> extends PTransform<PCollection<String>, PCollection<A>> {
  private final TypeDescriptor<T> typeDescriptor;
  private final SerializableFunction<String, T> parser;
  private final DoFn<Iterable<T>, A> averagingFunction;
  private final int batchSizeforAverage;

  public STATSPipeline(
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<String, T> parser,
      DoFn<Iterable<T>, A> averagingFunction,
      int batchSizeforAverage) {
    this.typeDescriptor = typeDescriptor;
    this.parser = parser;
    this.averagingFunction = averagingFunction;
    this.batchSizeforAverage = batchSizeforAverage;
  }

  @Override
  public PCollection<A> expand(PCollection<String> input) {
    return input
        .apply("Parse", MapElements.into(this.typeDescriptor).via(this.parser))
        .apply("Average", new Average<>(this.averagingFunction, batchSizeforAverage));
  }
}
