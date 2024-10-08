package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.SourceEntry;
import at.ac.uibk.dps.streamprocessingapplications.genevents.logging.BatchedFileLogging;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import org.apache.beam.sdk.transforms.DoFn;

public class TimerSourceBeam extends DoFn<String, SourceEntry> {
  BlockingQueue<List<String>> eventQueue;
  String csvFileName;
  String outSpoutCSVLogFileName;
  String experiRunId;
  double scalingFactor;
  long msgId;

  public TimerSourceBeam(
      String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, String experiRunId) {
    this.csvFileName = csvFileName;
    this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
    this.scalingFactor = scalingFactor;
    this.experiRunId = experiRunId;
  }

  public TimerSourceBeam(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor) {
    this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
  }

  @Setup
  public void setup() {
    BatchedFileLogging.writeToTemp(this, this.outSpoutCSVLogFileName);
    Random r = new Random();
    try {
      if (InetAddress.getLocalHost().getHostName().compareTo("anshudreamd2") == 0)
        msgId = (long) (1 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshustormscsup2d1") == 0)
        msgId = (long) (2 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshustormscsup4d1") == 0)
        msgId = (long) (3 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup4") == 0)
        msgId = (long) (4 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup5") == 0)
        msgId = (long) (5 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup6") == 0)
        msgId = (long) (6 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup7") == 0)
        msgId = (long) (7 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup8") == 0)
        msgId = (long) (8 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup9") == 0)
        msgId = (long) (9 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup10") == 0)
        msgId =
            (long) (10 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup11") == 0)
        msgId =
            (long) (11 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup12") == 0)
        msgId =
            (long) (12 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup13") == 0)
        msgId =
            (long) (13 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup14") == 0)
        msgId =
            (long) (14 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup15") == 0)
        msgId =
            (long) (15 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup16") == 0)
        msgId =
            (long) (16 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup17") == 0)
        msgId =
            (long) (17 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup18") == 0)
        msgId =
            (long) (18 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup19") == 0)
        msgId =
            (long) (19 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup20") == 0)
        msgId =
            (long) (20 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup21") == 0)
        msgId =
            (long) (21 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup22") == 0)
        msgId =
            (long) (22 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup23") == 0)
        msgId =
            (long) (23 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup24") == 0)
        msgId =
            (long) (24 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("Anshus-MacBook-Pro.local") == 0)
        msgId =
            (long) (24 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      if (InetAddress.getLocalHost().getHostName().compareTo("anshudreamd2.cloudapp.net") == 0)
        msgId =
            (long) (24 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
      else msgId = r.nextInt(10000);

    } catch (UnknownHostException e) {

      e.printStackTrace();
    }
  }

  @ProcessElement
  public void processElement(@Element String input, OutputReceiver<SourceEntry> out) {
    try {
      SourceEntry values = new SourceEntry();
      String ROWKEYSTART = input.split(",")[1];
      String ROWKEYEND = input.split(",")[2];
      values.setRowString(input);
      msgId++;
      values.setMsgid(Long.toString(msgId));
      values.setRowKeyStart(ROWKEYSTART);
      values.setRowKeyEnd(ROWKEYEND);
      values.setArrivalTime(System.currentTimeMillis());
      out.output(values);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
