package at.ac.uibk.dps.streamprocessingapplications.beam;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import at.ac.uibk.dps.streamprocessingapplications.entity.SourceEntry;
import at.ac.uibk.dps.streamprocessingapplications.genevents.ISyntheticEventGen;
import org.apache.beam.sdk.transforms.DoFn;
import at.ac.uibk.dps.streamprocessingapplications.genevents.EventGen;
import at.ac.uibk.dps.streamprocessingapplications.genevents.logging.BatchedFileLogging;

public class TimerSourceBeam extends DoFn<String, SourceEntry> implements ISyntheticEventGen {

    EventGen eventGen;
    BlockingQueue<List<String>> eventQueue;
    String csvFileName;
    String outSpoutCSVLogFileName;
    String experiRunId;
    double scalingFactor;
    BatchedFileLogging ba;
    long msgId;

    public TimerSourceBeam(
            String csvFileName,
            String outSpoutCSVLogFileName,
            double scalingFactor,
            String experiRunId) {
        this.csvFileName = csvFileName;
        this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
        this.scalingFactor = scalingFactor;
        this.experiRunId = experiRunId;
    }

    public TimerSourceBeam(
            String csvFileName, String outSpoutCSVLogFileName, double scalingFactor) {
        this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
    }

    @Setup
    public void setup() {
        // TODO Auto-generated method stub
        //		System.out.println("SampleSpout PID,"+ ManagementFactory.getRuntimeMXBean().getName());
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
        this.eventGen = new EventGen(this, this.scalingFactor);
        this.eventQueue = new LinkedBlockingQueue<List<String>>();
        String uLogfilename = this.outSpoutCSVLogFileName + msgId;
        this.eventGen.launch(this.csvFileName, uLogfilename); // Launch threads
    }

    @ProcessElement
    public void processElement(@Element String input, OutputReceiver<SourceEntry> out) {
        // TODO Auto-generated method stub
        //		try {
        //		System.out.println("spout Queue count= "+this.eventQueue.size());
        // allow multiple tuples to be emitted per next tuple.
        // Discouraged? https://groups.google.com/forum/#!topic/storm-user/SGwih7vPiDE
        int count = 0, MAX_COUNT = 10; // FIXME?
        while (count < MAX_COUNT) {
            List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
            if (entry == null) return;
            count++;
            SourceEntry values = new SourceEntry();
            StringBuilder rowStringBuf = new StringBuilder();
            for (String s : entry) {
                rowStringBuf.append(",").append(s);
            }
            String rowString = rowStringBuf.toString().substring(1);
            String ROWKEYSTART = rowString.split(",")[2];
            String ROWKEYEND = rowString.split(",")[3];
            ;
            System.out.println("rowString:" + rowString.split(",")[2]);

            values.setRowString(rowString);
            msgId++;
            values.setMsgid(Long.toString(msgId));

            values.setRowKeyStart(ROWKEYSTART);
            values.setRowKeyEnd(ROWKEYEND);

            // sending rowkeystart and rowkeyend
            //			rowStringBuf.
            //			rowString.split(",")[3];

            //

            out.output(values);
            //		} catch (InterruptedException e) {
            //			// TODO Auto-generated catch block
            //			e.printStackTrace();
            //		}
        }
    }

    @Override
    public void receive(List<String> event) {
        // TODO Auto-generated method stub
        // System.out.println("Called IN SPOUT### ");
        try {
            this.eventQueue.put(event);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
