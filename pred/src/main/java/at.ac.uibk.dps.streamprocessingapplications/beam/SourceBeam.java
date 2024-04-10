package at.ac.uibk.dps.streamprocessingapplications.beam;

import static java.time.Duration.ofMillis;
import static java.util.Collections.singleton;

import at.ac.uibk.dps.streamprocessingapplications.entity.SourceEntry;
import at.ac.uibk.dps.streamprocessingapplications.kafka.MyKafkaConsumer;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceBeam extends DoFn<String, SourceEntry> implements ISyntheticEventGen {

    private static Logger l;

    BlockingQueue<List<String>> eventQueue;
    String csvFileName;
    String outSpoutCSVLogFileName;
    String experiRunId;
    long msgId;

    long numberLines;

    private final long POLL_TIMEOUT_MS = 1000;

    private MyKafkaConsumer myKafkaConsumer;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    public SourceBeam(
            String csvFileName,
            String outSpoutCSVLogFileName,
            String experiRunId,
            long lines,
            String bootstrap,
            String topic) {
        this.csvFileName = csvFileName;
        this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
        this.experiRunId = experiRunId;
        this.myKafkaConsumer = new MyKafkaConsumer(bootstrap, "group-" +UUID.randomUUID(), 10000, topic);
        this.numberLines = lines;
    }

    public SourceBeam(
            String csvFileName,
            String outSpoutCSVLogFileName,
            long lines,
            String bootstrap,
            String topic) {
        this(csvFileName, outSpoutCSVLogFileName, "", lines, bootstrap, topic);
    }

    @Setup
    public void setup() {
        Random r = new Random();
        try {
            msgId =
                    (long)
                            (1 * Math.pow(10, 12)
                                    + (r.nextInt(1000) * Math.pow(10, 9))
                                    + r.nextInt(10));

        } catch (Exception e) {

            e.printStackTrace();
        }
        // this.eventGen = new EventGen(this, this.scalingFactor);
        // this.eventQueue = new LinkedBlockingQueue<>();
        // String uLogfilename = this.outSpoutCSVLogFileName + msgId;
        boolean isJson = csvFileName.contains("senml");
        initLogger(LoggerFactory.getLogger("APP"));

        // this.eventGen.launch(this.csvFileName, uLogfilename, -1, isJson); // Launch threads
    }

    @ProcessElement
    public void processElement(@Element String input, OutputReceiver<SourceEntry> out)
            throws IOException {
        long count = 1, MAX_COUNT = 100; // FIXME?
        KafkaConsumer<Long, byte[]> kafkaConsumer;
        kafkaConsumer = myKafkaConsumer.createKafkaConsumer();
        kafkaConsumer.subscribe(singleton(myKafkaConsumer.getTopic()), myKafkaConsumer);

        while (count < numberLines) {
            /*
            List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
            if (entry == null) {
                // return;
                continue;
            }
             */

            try {
                ConsumerRecords<Long, byte[]> records =
                        kafkaConsumer.poll(ofMillis(POLL_TIMEOUT_MS));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<Long, byte[]> record : records) {
                        SourceEntry values = new SourceEntry();
                        String rowString = new String(record.value());
                        String newRow = rowString.substring(rowString.indexOf(",") + 1);
                        values.setMsgid(Long.toString(msgId));
                        values.setPayLoad(newRow);
                        out.output(values);
                        msgId++;
                        count++;
                    }
                }
            } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                // Handle invalid offset or no offset found errors when auto.reset.policy is not set
                System.out.println(
                        "Invalid or no offset found, and auto.reset.policy unset, using latest");
                throw new RuntimeException(e);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void receive(List<String> event) {
        try {
            this.eventQueue.put(event);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
