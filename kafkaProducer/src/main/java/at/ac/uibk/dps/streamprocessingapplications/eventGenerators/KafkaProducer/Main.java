package at.ac.uibk.dps.streamprocessingapplications.eventGenerators.KafkaProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import sun.misc.Signal;

public class Main {
  public static void main(String[] args) {
    if (args.length != 3
        || args[0].trim().isEmpty()
        || args[1].trim().isEmpty()
        || args[2].trim().isEmpty()) {
      System.out.println("Usage: <path_to_csv> <number_of_threads> <kafka_topic>");
      return;
    }

    final String FILE_PATH = args[0];
    final int THREAD_COUNT = Integer.parseInt(args[1]);
    final String KAFKA_TOPIC = args[2];

    final Properties kafkaProperties = new Properties();
    kafkaProperties.put("bootstrap.servers", "localhost:9093");
    kafkaProperties.put("key.serializer", LongSerializer.class.getName());
    kafkaProperties.put("value.serializer", StringSerializer.class.getName());

    final AtomicBoolean running = new AtomicBoolean(true);

    List<Thread> threads = new ArrayList<>(THREAD_COUNT);
    long unixStartTime = System.currentTimeMillis();
    for (int i = 0; i < THREAD_COUNT; i++) {
      Thread thread =
          new Thread(
              new EventProducer(
                  running,
                  unixStartTime,
                  FILE_PATH,
                  i,
                  THREAD_COUNT - 1,
                  KAFKA_TOPIC,
                  kafkaProperties));
      threads.add(thread);
      thread.start();
    }

    Signal.handle(
        new Signal("INT"), // SIGINT
        signal -> {
          System.out.println("\nDetected SIGINT, shutting down...");
          running.set(false);
        });

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    System.out.println("All threads finished.");
  }
}
