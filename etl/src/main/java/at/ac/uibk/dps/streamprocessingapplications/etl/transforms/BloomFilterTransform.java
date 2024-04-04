package at.ac.uibk.dps.streamprocessingapplications.etl.taxi.transforms;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.BloomFilter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Funnel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BloomFilterTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

  private static final Logger LOGGER = LogManager.getLogger(BloomFilterTransform.class);
  private final Optional<BloomFilter<T>> bloomFilter;

  public BloomFilterTransform(BloomFilter<T> bloomFilter) {
    this.bloomFilter = Optional.ofNullable(bloomFilter);
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    if (this.bloomFilter.isEmpty()) {
      LOGGER.info("Bloom filter not available. Falling back to no op.");
      return input;
    }
    return input.apply(Filter.by(this.bloomFilter.get()::mightContain));
  }

  private Optional<BloomFilter<String>> buildBloomFromResource(String filePath) {
    /* WARN:
     * Do NOT replace this funnel! It has to be the same as the one used to create the serialized model.
     * The bloom filter model file was adopted from the `riot-bench` repository
     * and this is their funnel implementation.
     */
    final Funnel<String> FUNNEL = (memberId, sink) -> sink.putString(memberId, Charsets.UTF_8);
    ClassLoader classLoader = getClass().getClassLoader();
    BloomFilter<String> bloomFilter;

    try (InputStream inputStream = classLoader.getResourceAsStream(filePath)) {
      if (inputStream == null) {
        LOGGER.error(String.format("`InputStream` from path \"%s\" is null.", filePath));
        return Optional.empty();
      }
      bloomFilter = BloomFilter.readFrom(inputStream, FUNNEL);
    } catch (IOException e) {
      LOGGER.error("Unable to build `BloomFilter` from input stream.");
      return Optional.empty();
    }

    return Optional.of(bloomFilter);
  }
}
