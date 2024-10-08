package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.AverageEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.SenMlEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.BlockWindowAverage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;

public class AverageBeam extends DoFn<SenMlEntry, AverageEntry> {

  private static Logger l;
  private final String dataSetType;
  Map<String, BlockWindowAverage> blockWindowAverageMap;
  private Properties p;
  private ArrayList<String> useMsgList;

  public AverageBeam(Properties p_, String dataSetType) {
    p = p_;
    this.dataSetType = dataSetType;
  }

  public static void initLogger(Logger l_) {
    l = l_;
  }

  @Setup
  public void setup() throws MqttException {
    blockWindowAverageMap = new HashMap<>();
    String useMsgField = p.getProperty("AGGREGATE.BLOCK_AVERAGE.USE_MSG_FIELD");
    String[] msgField = useMsgField.split(",");
    useMsgList = new ArrayList<>();
    for (String s : msgField) {
      useMsgList.add(s);
    }
  }

  @ProcessElement
  public void processElement(@Element SenMlEntry input, DoFn.OutputReceiver<AverageEntry> out)
      throws IOException {
    String msgId = input.getMsgid();
    String sensorMeta = input.getMeta();
    String sensorID = input.getSensorID();
    String obsType = input.getObsType();
    String obsVal = input.getObsVal();

    HashMap<String, String> map = new HashMap<>();
    if (dataSetType.equals("TAXI")) {
      // obsVal = "12,13,14";
      String fare_amount = obsVal.split(",")[2]; // fare_amount as last obs. in input
      map.put(AbstractTask.DEFAULT_KEY, fare_amount);
    }

    if (dataSetType.equals("SYS")) {
      String airquality = obsVal.split(",")[4]; // airquality as last obs. in input
      map.put(AbstractTask.DEFAULT_KEY, airquality);
    }

    if (dataSetType.equals("FIT")) {
      String fare_amount = obsVal.split(",")[7];
      map.put(AbstractTask.DEFAULT_KEY, fare_amount);
    }

    if (useMsgList.contains(obsType)) {
      String key = sensorID + obsType;
      BlockWindowAverage blockWindowAverage = blockWindowAverageMap.get(key);
      if (blockWindowAverage == null) {
        blockWindowAverage = new BlockWindowAverage();
        blockWindowAverage.setup(l, p);
        blockWindowAverageMap.put(key, blockWindowAverage);
      }

      blockWindowAverage.doTask(map);

      Float avgres =
          blockWindowAverage.getLastResult(); //  Avg of last window is used till next comes
      sensorMeta = sensorMeta.concat(",").concat(obsType);

      if (dataSetType.equals("TAXI") | dataSetType.equals("FIT")) {
        obsType = "fare_amount";
      }
      if (dataSetType.equals("SYS")) {
        obsType = "AVG";
      }

      if (avgres != null) {
        if (avgres != Float.MIN_VALUE) {
          if (l.isInfoEnabled()) l.info("avgres AVG:{}", avgres);
          AverageEntry averageEntry =
              new AverageEntry(
                  sensorMeta, sensorID, obsType, avgres.toString(), obsVal, msgId, "AVG");
          averageEntry.setArrivalTime(input.getArrivalTime());
          out.output(averageEntry);
        } else {
          if (l.isWarnEnabled()) l.warn("Error in BlockWindowAverageBolt");
          throw new RuntimeException();
        }
      }
    }
  }
}
