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

    public SourceBeam(
            String csvFileName,
            String outSpoutCSVLogFileName,
            double scalingFactor,
            String experiRunId) {
        this.csvFileName = csvFileName;
        this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
        this.scalingFactor = scalingFactor;
        this.experiRunId = experiRunId;
    }

    public SourceBeam(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor) {
        this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
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
        this.eventQueue = new LinkedBlockingQueue<List<String>>();
        String uLogfilename = this.outSpoutCSVLogFileName + msgId;
        this.eventGen.launch(this.csvFileName, uLogfilename, -1, true); // Launch threads
    }

    @ProcessElement
    public void processElement(@Element String input, OutputReceiver<SourceEntry> out)
            throws IOException {
        int count = 0, MAX_COUNT = 100; // FIXME?
        while (count < MAX_COUNT) {
            List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
            if (entry == null) {
                // return;
                continue;
            }
            count++;
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

    @Teardown
    public void cleanUp() {
        this.eventGen.tearDown();
    }
}
