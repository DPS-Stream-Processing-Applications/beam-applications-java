package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.LinearRegressionEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.SenMlEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.LinearRegressionPredictor;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinearRegressionBeam1 extends DoFn<SenMlEntry, LinearRegressionEntry> {

  private static Logger l;
  private final String dataSetType;
  LinearRegressionPredictor linearRegressionPredictor;
  private Properties p;

  private final String databaseUrl;

  private final String databaseName;

  public LinearRegressionBeam1(
      Properties p_, String dataSetType, String databaseUrl, String databaseName) {
    p = p_;
    this.dataSetType = dataSetType;
    this.databaseName = databaseName;
    this.databaseUrl = databaseUrl;
  }

  public static void initLogger(Logger l_) {
    l = l_;
  }

  @Setup
  public void setup() throws MqttException {
    linearRegressionPredictor = new LinearRegressionPredictor(databaseUrl, databaseName);
    initLogger(LoggerFactory.getLogger("APP"));
    linearRegressionPredictor.setup(l, p);
  }

  @ProcessElement
  public void processElement(@Element SenMlEntry input, OutputReceiver<LinearRegressionEntry> out)
      throws IOException {

    String sensorMeta = input.getMeta();
    String msgType = input.getMsgtype();

    String obsVal = "";

    if (dataSetType.equals("TAXI") | dataSetType.equals("FIT")) {
      obsVal = "10,1955.22,27";
    } else {
      obsVal = "22.7,49.3,0,1955.22,27";
    }

    String msgId = "0";

    if (!msgType.equals("modelupdate")) {
      obsVal = input.getObsVal();
      msgId = input.getMsgid();

      if (l.isInfoEnabled()) l.info("modelupdate obsVal-" + obsVal);
    }

    HashMap<String, String> map = new HashMap<>();
    map.put(AbstractTask.DEFAULT_KEY, obsVal);
    Float res = linearRegressionPredictor.doTask(map);
    if (l.isInfoEnabled()) l.info("res linearRegressionPredictor-" + res);

    if (res != null) {
      if (res != Float.MIN_VALUE) {
        LinearRegressionEntry linearRegressionEntry =
            new LinearRegressionEntry(sensorMeta, obsVal, msgId, res.toString(), "MLR");
        linearRegressionEntry.setArrivalTime(input.getArrivalTime());
        out.output(linearRegressionEntry);
      } else {
        if (l.isWarnEnabled()) l.warn("Error in LinearRegressionPredictorBolt");
        throw new RuntimeException("Res is null or float.min");
      }
    }
  }
}
