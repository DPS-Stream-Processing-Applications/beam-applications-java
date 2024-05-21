package at.ac.uibk.dps.streamprocessingapplications.eventGenerators.KafkaProducer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventConsumer implements Runnable {
  private BlockingQueue<Event> eventQueue;
  private Properties kafkaProperties;
  private String topic;

  public EventConsumer(BlockingQueue<Event> eventQueue, Properties kafkaProperties, String topic) {
    this.eventQueue = eventQueue;
    this.kafkaProperties = kafkaProperties;
    this.topic = topic;
  }

  @Override
  public void run() {
    while (!eventQueue.isEmpty()) {
      try {
        Event event = eventQueue.take();
        try (Producer<Long, String> producer = new KafkaProducer<>(this.kafkaProperties)) {
          producer.send(
              new ProducerRecord<>(topic, Instant.now().getEpochSecond(), event.getEventString()));
          // System.out.println("sent");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
