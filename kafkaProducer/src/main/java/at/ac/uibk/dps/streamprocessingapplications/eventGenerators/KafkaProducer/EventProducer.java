package at.ac.uibk.dps.streamprocessingapplications.eventGenerators.KafkaProducer;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer implements Runnable {
  private final AtomicBoolean running;
  private final long unixStartTime;
  private static final char SEPARATION_CHARACTER = '|';
  private final int STRIDE;
  private CSVReader reader;
  private final Properties kafkaProperties;
  private final String kafkaTopic;

  public EventProducer(
      AtomicBoolean running,
      long unixStartTime,
      String filePath,
      int initialOffset,
      int stride,
      String kafkaTopic,
      Properties kafkaProperties) {
    this.running = running;
    this.unixStartTime = unixStartTime;
    this.STRIDE = stride;
    this.kafkaProperties = kafkaProperties;
    this.kafkaTopic = kafkaTopic;
    try {
      this.reader =
          new CSVReaderBuilder(new FileReader(filePath))
              .withCSVParser(new CSVParserBuilder().withSeparator(SEPARATION_CHARACTER).build())
              .build();

      this.reader.skip(initialOffset);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {
    try (Producer<Long, String> kafkaProducer = new KafkaProducer<>(this.kafkaProperties)) {
      String[] csvLine = reader.readNext();
      while (running.get() && csvLine != null) {
        long requiredElapsedTime = Long.parseLong(csvLine[0]);

        while (running.get()
            && requiredElapsedTime - (System.currentTimeMillis() - this.unixStartTime) > 0) {
          Thread.sleep(100);
        }
        if (!running.get()) {
          break;
        }

        kafkaProducer.send(
            new ProducerRecord<>(this.kafkaTopic, System.currentTimeMillis(), csvLine[1]));
        reader.skip(this.STRIDE);
        csvLine = reader.readNext();
      }
      // Cleanup
      reader.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
