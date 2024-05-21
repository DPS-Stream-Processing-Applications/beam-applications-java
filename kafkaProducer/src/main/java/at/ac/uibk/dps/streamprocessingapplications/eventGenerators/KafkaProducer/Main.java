package at.ac.uibk.dps.streamprocessingapplications.eventGenerators.KafkaProducer;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Main {
  public static void main(String[] args) {
    if (args.length == 0 || args[0].trim().isEmpty()) {
      // System.out.println("You need to specify a path!");
      return;
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9093");
    props.put("key.serializer", LongSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    String topic = "senml-source";

    BlockingQueue<Event> eventQueue = new DelayQueue<>();
    ExecutorService producer = Executors.newFixedThreadPool(1);
    CSVParser parser = new CSVParserBuilder().withSeparator('|').build();
    CSVReader reader;

    try {
      reader =
          new CSVReaderBuilder(new FileReader(args[0]))
              .withSkipLines(1)
              .withCSVParser(parser)
              .build();
      producer.submit(new EventProducer(eventQueue, reader));
    } catch (FileNotFoundException e) {
      // System.out.println("file_not_found");
    }
    // HACK for initial startup...
    while (eventQueue.isEmpty()) {}

    ExecutorService consumers = Executors.newFixedThreadPool(64);
    for (int i = 0; i < 4; i++) {
      consumers.submit(new EventConsumer(eventQueue, props, topic));
    }

    try {
      producer.awaitTermination(3, TimeUnit.SECONDS);
      consumers.awaitTermination(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    producer.shutdown();
    consumers.shutdown();

    // TODO: Fix infinite runtime
  }
}
