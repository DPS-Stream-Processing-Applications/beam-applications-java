package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.SourceEntry;
import at.ac.uibk.dps.streamprocessingapplications.genevents.EventGen;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.beam.sdk.transforms.DoFn;

public class SourceBeam extends DoFn<String, SourceEntry> implements ISyntheticEventGen {

    BlockingQueue<List<String>> eventQueue;
    String csvFileName;
    String outSpoutCSVLogFileName;
    String experiRunId;
    double scalingFactor;
    EventGen eventGen;
    long msgId;

    long numberLines;

    public SourceBeam(
            String csvFileName,
            String outSpoutCSVLogFileName,
            double scalingFactor,
            String experiRunId,
            long lines) {
        this.csvFileName = csvFileName;
        this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
        this.scalingFactor = scalingFactor;
        this.experiRunId = experiRunId;
        this.numberLines = lines;
    }

    public SourceBeam(
            String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, long lines) {
        this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "", lines);
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
        this.eventGen = new EventGen(this, this.scalingFactor);
        this.eventQueue = new LinkedBlockingQueue<>();
        String uLogfilename = this.outSpoutCSVLogFileName + msgId;
        this.eventGen.launch(this.csvFileName, uLogfilename, -1, true); // Launch threads
    }

    @ProcessElement
    public void processElement(@Element String input, OutputReceiver<SourceEntry> out)
            throws IOException {
        long count = 1, MAX_COUNT = 100; // FIXME?
        while (count < numberLines) {
            List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
            if (entry == null) {
                // return;
                continue;
            }
            System.out.println("Count: " + count);
            System.out.println(count);
            SourceEntry values = new SourceEntry();
            StringBuilder rowStringBuf = new StringBuilder();
            for (String s : entry) {
                rowStringBuf.append(",").append(s);
            }
            String rowString = rowStringBuf.toString().substring(1);
            String newRow = rowString.substring(rowString.indexOf(",") + 1);
            msgId++;
            values.setMsgid(Long.toString(msgId));
            values.setPayLoad(newRow);
            out.output(values);
            count++;
        }
        System.out.println("left loop");
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
