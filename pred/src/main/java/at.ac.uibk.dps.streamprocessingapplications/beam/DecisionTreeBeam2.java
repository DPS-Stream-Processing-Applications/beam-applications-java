package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.DecisionTreeEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.SenMlEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.DecisionTreeClassify;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecisionTreeBeam2 extends DoFn<SenMlEntry, DecisionTreeEntry> {
  private static Logger l;
  DecisionTreeClassify decisionTreeClassify;
  private Properties p;
  private String dataSetType;

  public DecisionTreeBeam2(Properties p_, String dataSetType) {
    p = p_;
    this.dataSetType = dataSetType;
  }

  public static void initLogger(Logger l_) {
    l = l_;
  }

  @Setup
  public void setup() throws MqttException {
    initLogger(LoggerFactory.getLogger("APP"));
    decisionTreeClassify = new DecisionTreeClassify(dataSetType);
    decisionTreeClassify.setup(l, p);
  }

  @ProcessElement
  public void processElement(@Element SenMlEntry input, DoFn.OutputReceiver<DecisionTreeEntry> out)
      throws IOException {
    String sensorMeta = input.getMeta();

    String obsVal = "0";
    String msgId = "0";
    if (dataSetType.equals("FIT") | dataSetType.equals("TAXI")) {
      obsVal = "10,1955.22,27"; // dummy
    }
    if (dataSetType.equals("SYS")) {
      obsVal = "22.7,49.3,0,1955.22,27"; // dummy
    }

    /* We are getting a model update message so we will update the model only*/

    //        if(msgtype.equals("modelupdate") && analyticsType.equals("DTC")){
    //
    //            try {
    //                lr = (LinearRegression) SerializationHelper.read(BlobModelObject);
    //            } catch (Exception e) {
    //                e.printStackTrace();
    //            }
    //            if(this.l.isInfoEnabled()) {
    //                this.l.info("Model is {} ", lr.toString());
    //            }
    //        }

    HashMap<String, String> map = new HashMap<>();
    map.put(AbstractTask.DEFAULT_KEY, obsVal);
    Float res = decisionTreeClassify.doTask(map); // index of result-class/enum as return
    if (res != null) {
      if (res != Float.MIN_VALUE)
        out.output(new DecisionTreeEntry(sensorMeta, obsVal, msgId, res.toString(), "DTC"));
      else {
        if (l.isWarnEnabled()) l.warn("Error in DecisionTreeClassifyBolt");
        throw new RuntimeException();
      }
    }
  }
}
