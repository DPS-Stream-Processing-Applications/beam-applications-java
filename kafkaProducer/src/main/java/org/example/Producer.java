package org.example;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;

public class Producer implements Callback {
    private String BOOTSTRAP_SERVERS;

    protected AtomicLong messageCount = new AtomicLong(0);

    public Producer(String BOOTSTRAP_SERVERS) {
        this.BOOTSTRAP_SERVERS = BOOTSTRAP_SERVERS;
    }

    public void run(String message, String topic) {
        try (var producer = createKafkaProducer()) {
            ProducerRecord<Long, byte[]> test =
                    new ProducerRecord<>(topic, messageCount.get(), message.getBytes());
            producer.send(test, this);
            messageCount.incrementAndGet();
        }
    }

    private KafkaProducer<Long, byte[]> createKafkaProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new KafkaProducer<>(props);
    }

    private void sleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
}
