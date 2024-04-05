package at.ac.uibk.dps.streamprocessingapplications.beam;

import static java.time.Duration.ofMillis;
import static java.util.Collections.singleton;

import at.ac.uibk.dps.streamprocessingapplications.entity.SourceEntry;
import at.ac.uibk.dps.streamprocessingapplications.genevents.EventGen;
import at.ac.uibk.dps.streamprocessingapplications.genevents.ISyntheticEventGen;
import at.ac.uibk.dps.streamprocessingapplications.genevents.logging.BatchedFileLogging;
import at.ac.uibk.dps.streamprocessingapplications.kafka.MyKafkaConsumer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.kafka.clients.consumer.*;

public class TimerSourceBeam extends DoFn<String, SourceEntry> implements ISyntheticEventGen {

    EventGen eventGen;
    BlockingQueue<List<String>> eventQueue;
    String csvFileName;
    String outSpoutCSVLogFileName;
    String experiRunId;
    double scalingFactor;
    long msgId;

    long numberLines;

    private final long POLL_TIMEOUT_MS = 1000;

    private final String TOPIC_NAME;

    private MyKafkaConsumer myKafkaConsumer;

    public TimerSourceBeam(
            String csvFileName,
            String outSpoutCSVLogFileName,
            double scalingFactor,
            String experiRunId,
            long lines,
            String bootstrapserver,
            String topic) {
        this.csvFileName = csvFileName;
        this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
        this.scalingFactor = scalingFactor;
        this.experiRunId = experiRunId;
        this.numberLines = lines;
        this.myKafkaConsumer = new MyKafkaConsumer(bootstrapserver, "group-1", 1000, topic, lines);
        this.TOPIC_NAME = topic;
    }

    public TimerSourceBeam(
            String csvFileName,
            String outSpoutCSVLogFileName,
            double scalingFactor,
            long lines,
            String bootstrapserver,
            String topic) {
        this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "", lines, bootstrapserver, topic);
    }

    @Setup
    public void setup() {
        BatchedFileLogging.writeToTemp(this, this.outSpoutCSVLogFileName);
        Random r = new Random();
        try {
            if (InetAddress.getLocalHost().getHostName().compareTo("anshudreamd2") == 0)
                msgId =
                        (long)
                                (1 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshustormscsup2d1") == 0)
                msgId =
                        (long)
                                (2 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshustormscsup4d1") == 0)
                msgId =
                        (long)
                                (3 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup4") == 0)
                msgId =
                        (long)
                                (4 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup5") == 0)
                msgId =
                        (long)
                                (5 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup6") == 0)
                msgId =
                        (long)
                                (6 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup7") == 0)
                msgId =
                        (long)
                                (7 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup8") == 0)
                msgId =
                        (long)
                                (8 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup9") == 0)
                msgId =
                        (long)
                                (9 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup10") == 0)
                msgId =
                        (long)
                                (10 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup11") == 0)
                msgId =
                        (long)
                                (11 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup12") == 0)
                msgId =
                        (long)
                                (12 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup13") == 0)
                msgId =
                        (long)
                                (13 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup14") == 0)
                msgId =
                        (long)
                                (14 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup15") == 0)
                msgId =
                        (long)
                                (15 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup16") == 0)
                msgId =
                        (long)
                                (16 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup17") == 0)
                msgId =
                        (long)
                                (17 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup18") == 0)
                msgId =
                        (long)
                                (18 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup19") == 0)
                msgId =
                        (long)
                                (19 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup20") == 0)
                msgId =
                        (long)
                                (20 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup21") == 0)
                msgId =
                        (long)
                                (21 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup22") == 0)
                msgId =
                        (long)
                                (22 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup23") == 0)
                msgId =
                        (long)
                                (23 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup24") == 0)
                msgId =
                        (long)
                                (24 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("Anshus-MacBook-Pro.local") == 0)
                msgId =
                        (long)
                                (24 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            if (InetAddress.getLocalHost().getHostName().compareTo("anshudreamd2.cloudapp.net")
                    == 0)
                msgId =
                        (long)
                                (24 * Math.pow(10, 12)
                                        + (r.nextInt(1000) * Math.pow(10, 9))
                                        + r.nextInt(10));
            else msgId = r.nextInt(10000);

        } catch (UnknownHostException e) {

            e.printStackTrace();
        }

        //		msgId=r.nextInt(10000);
        // this.eventGen = new EventGen(this, this.scalingFactor);
        // this.eventQueue = new LinkedBlockingQueue<List<String>>();
        // String uLogfilename = this.outSpoutCSVLogFileName + msgId;
        // this.eventGen.launch(this.csvFileName, uLogfilename); // Launch threads

        // ba = new BatchedFileLogging(uLogfilename, "test");
    }

    @ProcessElement
    public void processElement(@Element String input, OutputReceiver<SourceEntry> out) {
        // allow multiple tuples to be emitted per next tuple.
        // Discouraged? https://groups.google.com/forum/#!topic/storm-user/SGwih7vPiDE
        // int count = 0, MAX_COUNT = 10; // FIXME?
        KafkaConsumer<Long, byte[]> kafkaConsumer;
        kafkaConsumer = myKafkaConsumer.createKafkaConsumer();
        kafkaConsumer.subscribe(singleton(TOPIC_NAME), myKafkaConsumer);

        int sendMessages = 0;
        while (sendMessages < numberLines) {
            /*
            List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
            if (entry == null) {
                // System.out.println("I am exiting");
                // return;
                continue;
            }

             */
            // count++;
            /*
            SourceEntry values = new SourceEntry();
            StringBuilder rowStringBuf = new StringBuilder();
            for (String s : entry) {
                rowStringBuf.append(",").append(s);
            }
             */

            try {
                // Poll for new records from Kafka
                ConsumerRecords<Long, byte[]> records =
                        kafkaConsumer.poll(ofMillis(POLL_TIMEOUT_MS));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<Long, byte[]> record : records) {
                        SourceEntry values = new SourceEntry();
                        String rowString = new String(record.value());
                        String ROWKEYSTART = rowString.split(",")[2];
                        String ROWKEYEND = rowString.split(",")[3];
                        values.setRowString(rowString);
                        msgId++;
                        values.setMsgid(Long.toString(msgId));
                        values.setRowKeyStart(ROWKEYSTART);
                        values.setRowKeyEnd(ROWKEYEND);
                        out.output(values);
                        sendMessages++;
                    }
                }
            } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                // Handle invalid offset or no offset found errors when auto.reset.policy is not set
                System.out.println(
                        "Invalid or no offset found, and auto.reset.policy unset, using latest");
                throw new RuntimeException(e);
            } catch (Exception e) {
                // Handle other exceptions, including retriable ones
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
            throw new RuntimeException("Exception in receive Event" + e);
        }
    }
}