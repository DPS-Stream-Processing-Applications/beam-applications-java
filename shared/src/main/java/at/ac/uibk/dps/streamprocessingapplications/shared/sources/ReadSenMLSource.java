package at.ac.uibk.dps.streamprocessingapplications.shared.sources;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ReadSenMLSource extends PTransform<PBegin, PCollection<String>> {
  String topic;

  public ReadSenMLSource(String topic) {

    this.topic = topic;
  }

  @Override
  public PCollection<String> expand(PBegin input) {
    return input
        .apply(
            "Read from Kafka source",
            KafkaIO.<Long, String>read()
                .withBootstrapServers("kafka-cluster-kafka-bootstrap:9092")
                .withTopic(this.topic)
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withReadCommitted()
                .withoutMetadata())
        .apply(Values.create());
  }
}
