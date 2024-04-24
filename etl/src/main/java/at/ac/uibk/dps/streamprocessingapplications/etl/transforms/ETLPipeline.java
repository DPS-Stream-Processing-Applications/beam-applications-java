package at.ac.uibk.dps.streamprocessingapplications.etl.transforms;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.BloomFilter;

public class ETLPipeline<T> extends PTransform<PCollection<String>, PCollection<T>> {
  private final TypeDescriptor<T> typeDescriptor;
  private final SerializableFunction<String, T> parser;
  private final SerializableFunction<T, T> rangeFilter;
  private final BloomFilter<T> bloomFilter;
  private final SerializableFunction<Iterable<T>, Iterable<T>> interpolator;
  private final int interpolationBatchSize;
  private final SerializableFunction<T, T> annotationFunction;

  public ETLPipeline(
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<String, T> parser,
      SerializableFunction<T, T> rangeFilter,
      BloomFilter<T> bloomFilter,
      SerializableFunction<Iterable<T>, Iterable<T>> interpolator,
      int interpolationBatchSize,
      SerializableFunction<T, T> annotationFunction) {
    this.typeDescriptor = typeDescriptor;
    this.parser = parser;
    this.rangeFilter = rangeFilter;
    this.bloomFilter = bloomFilter;
    this.interpolator = interpolator;
    this.interpolationBatchSize = interpolationBatchSize;
    this.annotationFunction = annotationFunction;
  }

  @Override
  public PCollection<T> expand(PCollection<String> input) {
    return input
        .apply("Parse", MapElements.into(this.typeDescriptor).via(this.parser))
        .apply("RangeFilter", MapElements.into(this.typeDescriptor).via(this.rangeFilter))
        .apply("BloomFilter", new BloomFilterTransform<>(this.bloomFilter))
        .apply(
            "Interpolate",
            new Interpolate<>(this.typeDescriptor, this.interpolator, this.interpolationBatchSize))
        .apply("Annotate", MapElements.into(this.typeDescriptor).via(this.annotationFunction));
  }
}
