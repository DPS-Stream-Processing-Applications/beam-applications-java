package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class STATSPipeline<T, A> extends PTransform<PCollection<String>, PCollection<Long>> {
  private final TypeDescriptor<T> typeDescriptor;
  private final SerializableFunction<String, T> parser;
  private final DoFn<Iterable<T>, A> averagingFunction;
  private final DoFn<Iterable<T>, Long> distinctCountFunction;
  private final int batchSize;

  public STATSPipeline(
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<String, T> parser,
      DoFn<Iterable<T>, A> averagingFunction,
      DoFn<Iterable<T>, Long> distinctCountFunction,
      int batchSize) {
    this.typeDescriptor = typeDescriptor;
    this.parser = parser;
    this.averagingFunction = averagingFunction;
    this.distinctCountFunction = distinctCountFunction;
    this.batchSize = batchSize;
  }

  @Override
  public PCollection<Long> expand(PCollection<String> input) {

    PCollection<T> parsedObjects =
        input.apply("Parse", MapElements.into(this.typeDescriptor).via(this.parser));
    PCollection<A> averages =
        parsedObjects.apply("Average", new Average<>(this.averagingFunction, batchSize));
    PCollection<Long> distinctCounts =
        parsedObjects.apply(
            "Count Distinct", new DistinctCount<>(this.distinctCountFunction, this.batchSize));
    return distinctCounts;
  }
}
