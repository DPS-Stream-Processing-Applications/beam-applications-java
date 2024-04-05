package at.ac.uibk.dps.streamprocessingapplications.etl.transforms;

import java.util.function.Function;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.BloomFilter;

public class ETLPipeline<T> extends PTransform<PCollection<String>, PCollection<T>> {
  private final TypeDescriptor<T> typeDescriptor;
  private final Function<String, T> parser;
  private final Function<T, T> rangeFilter;
  private final BloomFilter<T> bloomFilter;
  private final Function<Iterable<T>, Iterable<T>> interpolator;

  public ETLPipeline(
      TypeDescriptor<T> typeDescriptor,
      Function<String, T> parser,
      Function<T, T> rangeFilter,
      BloomFilter<T> bloomFilter,
      Function<Iterable<T>, Iterable<T>> interpolator) {
    this.typeDescriptor = typeDescriptor;
    this.parser = parser;
    this.rangeFilter = rangeFilter;
    this.bloomFilter = bloomFilter;
    this.interpolator = interpolator;
  }

  @Override
  public PCollection<T> expand(PCollection<String> input) {
    return input
        .apply("Parse", MapElements.into(this.typeDescriptor).via(this.parser::apply))
        .apply("RangeFilter", MapElements.into(this.typeDescriptor).via(this.rangeFilter::apply))
        .apply("BloomFilter", new BloomFilterTransform<>(this.bloomFilter))
        .apply("Interpolate", new Interpolate<>(this.typeDescriptor, this.interpolator, 10));
  }


}
