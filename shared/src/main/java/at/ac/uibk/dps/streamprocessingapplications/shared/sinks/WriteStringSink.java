package at.ac.uibk.dps.streamprocessingapplications.shared.sinks;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.common.serialization.StringSerializer;

public class WriteStringSink extends PTransform<PCollection<String>, PDone> {

  String topic;

  public WriteStringSink(String topic) {

    this.topic = topic;
  }

  @Override
  public PDone expand(PCollection<String> input) {
    return input.apply(
        "Write SenML strings to Kafka",
        KafkaIO.<Object, String>write()
            .withBootstrapServers("kafka-cluster-kafka-bootstrap:9092")
            .withTopic(this.topic)
            .withValueSerializer(StringSerializer.class)
            .values());
  }
}
