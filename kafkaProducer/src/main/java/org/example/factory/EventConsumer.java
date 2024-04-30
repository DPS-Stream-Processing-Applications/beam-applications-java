package org.example.factory;

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
    System.out.println("in run");
    System.out.println(eventQueue.size());
    while (!eventQueue.isEmpty()) {
      try {
        Event event = eventQueue.take();
        System.out.println(event.getEventString());
        try (Producer<Long, String> producer = new KafkaProducer<>(this.kafkaProperties)) {
          producer.send(
              new ProducerRecord<>(topic, Instant.now().getEpochSecond(), event.getEventString()));
          System.out.println("sent");
        } catch (Exception e) {
          // Print error message and reason for failure
          System.err.println("Failed to send message: " + e.getMessage());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println("after run");
  }
}
