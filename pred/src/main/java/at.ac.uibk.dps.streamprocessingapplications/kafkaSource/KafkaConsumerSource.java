package at.ac.uibk.dps.streamprocessingapplications.kafkaSource;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

public class KafkaConsumerSource extends BoundedSource<String> {
  private final String kafkaBootstrapServers;
  private final String kafkaTopic;

  private final long numberOfLines;

  public KafkaConsumerSource(String kafkaBootstrapServers, String kafkaTopic, long numberOfLines) {
    this.kafkaBootstrapServers = kafkaBootstrapServers;
    this.kafkaTopic = kafkaTopic;
    this.numberOfLines = numberOfLines;
  }

  @Override
  public Coder<String> getOutputCoder() {
    return StringUtf8Coder.of();
  }

  @Override
  public BoundedReader<String> createReader(PipelineOptions options) {
    return new KafkaConsumerReader(kafkaBootstrapServers, kafkaTopic, numberOfLines);
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) {
    return 0;
  }

  @Override
  public List<? extends BoundedSource<String>> split(
      long desiredBundleSizeBytes, PipelineOptions options) {
    return Collections.singletonList(this);
  }
}
