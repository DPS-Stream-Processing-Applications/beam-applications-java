package at.ac.uibk.dps.streamprocessingapplications.kafka;

import static java.time.Duration.ofMillis;
import static java.util.Collections.singleton;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;

public class MyKafkaConsumer
        implements ConsumerRebalanceListener, OffsetCommitCallback, Serializable {
    private String BOOTSTRAP_SERVERS;
    private String GROUP_ID;
    private long POLL_TIMEOUT_MS;
    private String TOPIC_NAME;
    private long NUM_MESSAGES;

    private KafkaConsumer<Long, byte[]> kafkaConsumer;
    protected AtomicLong messageCount = new AtomicLong(0);
    private Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();

    public MyKafkaConsumer(
            String BOOTSTRAP_SERVERS,
            String GROUP_ID,
            long POLL_TIMEOUT_MS,
            String TOPIC_NAME,
            long NUM_MESSAGES) {
        this.BOOTSTRAP_SERVERS = BOOTSTRAP_SERVERS;
        this.GROUP_ID = GROUP_ID;
        this.POLL_TIMEOUT_MS = POLL_TIMEOUT_MS;
        this.TOPIC_NAME = TOPIC_NAME;
        this.NUM_MESSAGES = NUM_MESSAGES;
    }

    public void run() {
        System.out.println("Running consumer");

        // Create a Kafka consumer instance
        // This consumer receives messages from the Kafka topic asynchronously
        try (var consumer = createKafkaConsumer()) {
            kafkaConsumer = consumer;
            consumer.subscribe(singleton(TOPIC_NAME), this);
            System.out.printf("Subscribed to %s%n", TOPIC_NAME);
            // while (messageCount.get() < NUM_MESSAGES) {
            while (true) {
                try {
                    // Poll for new records from Kafka
                    ConsumerRecords<Long, byte[]> records =
                            consumer.poll(ofMillis(POLL_TIMEOUT_MS));
                    if (!records.isEmpty()) {
                        for (ConsumerRecord<Long, byte[]> record : records) {
                            System.out.printf(
                                    "Record fetched from %s-%d with offset %d%",
                                    record.topic(), record.partition(), record.offset());
                            // Track pending offsets for commit
                            pendingOffsets.put(
                                    new TopicPartition(record.topic(), record.partition()),
                                    new OffsetAndMetadata(record.offset() + 1, null));
                            if (messageCount.incrementAndGet() == NUM_MESSAGES) {
                                break;
                            }
                        }
                        // Commit pending offsets asynchronously
                        consumer.commitAsync(pendingOffsets, this);
                        pendingOffsets.clear();
                    }
                } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                    // Handle invalid offset or no offset found errors when auto.reset.policy is not
                    // set
                    System.out.println(
                            "Invalid or no offset found, and auto.reset.policy unset, using"
                                    + " latest");
                    consumer.seekToEnd(e.partitions());
                    consumer.commitSync();
                } catch (Exception e) {
                    // Handle other exceptions, including retriable ones
                    System.err.println(e.getMessage());
                    if (!retriable(e)) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            }
        }
    }

    public KafkaConsumer<Long, byte[]> createKafkaConsumer() {
        // Create properties for the Kafka consumer
        Properties props = new Properties();

        // Configure the connection to Kafka brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // Set a unique client ID for tracking
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());

        // Set a consumer group ID for the consumer
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // Configure deserializers for keys and values
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        // Disable automatic offset committing
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Set the offset reset behavior to start consuming from the earliest available offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }

    private void sleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean retriable(Exception e) {
        if (e == null) {
            return false;
        } else if (e instanceof IllegalArgumentException
                || e instanceof UnsupportedOperationException
                || !(e instanceof RebalanceInProgressException)
                || !(e instanceof RetriableException)) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.printf("Assigned partitions: %s%n", partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.printf("Revoked partitions: %s%n", partitions);
        kafkaConsumer.commitSync(pendingOffsets);
        pendingOffsets.clear();
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        System.out.printf("Lost partitions: %s", partitions);
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        if (e != null) {
            System.err.println("Failed to commit offsets");
            if (!retriable(e)) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}
