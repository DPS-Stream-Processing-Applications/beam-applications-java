package org.example;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;

public class Producer implements Callback {
    private final String BOOTSTRAP_SERVERS;

    private static Logger l;

    protected AtomicLong messageCount = new AtomicLong(0);

    public Producer(String BOOTSTRAP_SERVERS, Logger log) {
        this.BOOTSTRAP_SERVERS = BOOTSTRAP_SERVERS;
        l = log;
    }

    public void run(String message, String topic) {

        try (var producer = createKafkaProducer()) {
            ProducerRecord<Long, byte[]> test =
                    new ProducerRecord<>(topic, messageCount.get(), message.getBytes());
            producer.send(test, this);
            l.info("Send: " + messageCount.get() + " " + message);
            System.out.println("Send: " + messageCount.get() + " " + message);
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

    private boolean retryable(Exception e) {
        return e instanceof RetriableException;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        if (e != null) {
            System.err.println(e.getMessage());
            if (!retryable(e)) {
                l.warn(e.toString());
                System.exit(1);
            }
        }
    }
}
