package at.ac.uibk.dps.streamprocessingapplications.kafkaSource;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerReader extends BoundedSource.BoundedReader<String> {
  private static long counter = 1;
  private final KafkaConsumer<String, String> consumer;
  private final long numberOfLines;

  public KafkaConsumerReader(String kafkaBootstrapServers, String kafkaTopic, long numberOfLines) {
    this.numberOfLines = numberOfLines;
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "group" + UUID.randomUUID());

    consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(kafkaTopic));
  }

  @Override
  public boolean start() {
    return true;
  }

  @Override
  public boolean advance() {
    return numberOfLines > counter;
  }

  @Override
  public String getCurrent() {
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        String value = record.value();
        if (value != null) {
          counter++;
          return record.value();
        }
      }
    }
  }

  @Override
  public void close() {
    consumer.close();
  }

  @Override
  public BoundedSource<String> getCurrentSource() {
    return null;
  }
}
