package org.apache.flink.statefun.playground.java.greeter.kafka;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;

public class MyKafkaProducer extends AbstractTask<String, String>
    implements Callback, Serializable {
  private String bootstrapServer;
  private String topic;

  private static int useMsgField;

  protected AtomicLong messageCount = new AtomicLong(0);

  public MyKafkaProducer(String BOOTSTRAP_SERVERS, String topic, Properties p_) {
    this.bootstrapServer = BOOTSTRAP_SERVERS;
    this.topic = topic;
    useMsgField = Integer.parseInt(p_.getProperty("IO.MQTT_PUBLISH.USE_MSG_FIELD"));
  }

  private KafkaProducer<Long, byte[]> createKafkaProducer() {
    Properties props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

    return new KafkaProducer<>(props);
  }

  private boolean retriable(Exception e) {
    if (e instanceof IllegalArgumentException
        || e instanceof UnsupportedOperationException
        || !(e instanceof RetriableException)) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public void onCompletion(RecordMetadata metadata, Exception e) {
    if (e != null) {
      System.err.println(e.getMessage());

      if (!retriable(e)) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  @Override
  protected Float doTaskLogic(Map map) {
    String m = (String) map.get(AbstractTask.DEFAULT_KEY);
    String input;
    if (useMsgField > 0) {
      input = m.split(",")[useMsgField - 1];
    } else if (useMsgField == -1) {
      input = String.valueOf(ThreadLocalRandom.current().nextInt(100));
    } else input = m;

    try (KafkaProducer producer = createKafkaProducer()) {
      ProducerRecord<Long, byte[]> test =
          new ProducerRecord<>(topic, messageCount.get(), input.getBytes());
      producer.send(test, this);
      messageCount.incrementAndGet();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return 1f;
  }
}
