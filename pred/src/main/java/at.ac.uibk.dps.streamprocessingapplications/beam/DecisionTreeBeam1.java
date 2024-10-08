package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.BlobReadEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.DecisionTreeEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.DecisionTreeClassify;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.classifiers.trees.J48;
import weka.core.SerializationHelper;

public class DecisionTreeBeam1 extends DoFn<BlobReadEntry, DecisionTreeEntry> {

  private static Logger l;
  private final String dataSetType;
  DecisionTreeClassify decisionTreeClassify;
  private Properties p;

  public DecisionTreeBeam1(Properties p_, String dataSetType) {
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
  public void processElement(@Element BlobReadEntry input, OutputReceiver<DecisionTreeEntry> out)
      throws IOException {
    String msgtype = input.getMsgType();
    String analyticsType = input.getAnalyticType();
    String sensorMeta = input.getMeta();

    String obsVal = "0";
    String msgId = "0";
    if (dataSetType.equals("FIT") | dataSetType.equals("TAXI")) {
      obsVal = "10,1955.22,27"; // dummy
    }
    if (dataSetType.equals("SYS") | dataSetType.equals("FIT")) {
      obsVal = "22.7,49.3,0,1955.22,27"; // dummy
    }

    /* We are getting a model update message so we will update the model only*/

    if (msgtype.equals("modelupdate") && analyticsType.equals("DTC")) {
      byte[] BlobModelObject = input.getBlobModelObject();
      InputStream bytesInputStream = new ByteArrayInputStream(BlobModelObject);
      //        	ByteArrayInputStream BlobModelObject= (ByteArrayInputStream)
      // input.getValueByField("BlobModelObject");
      // do nothing for now
      //            byte[] blobModelObjects = input.getBinaryByField("BlobModelObject");
      //            p.setProperty("CLASSIFICATION.DECISION_TREE.MODEL_PATH")
      try {

        DecisionTreeClassify.j48tree = (J48) SerializationHelper.read(bytesInputStream);
        if (l.isInfoEnabled()) l.info("Model is updated DTC {}", DecisionTreeClassify.j48tree);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

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
      if (res != Float.MIN_VALUE) {
        DecisionTreeEntry decisionTreeEntry =
            new DecisionTreeEntry(sensorMeta, obsVal, msgId, res.toString(), "DTC");
        decisionTreeEntry.setArrivalTime(input.getArrivalTime());
        out.output(decisionTreeEntry);
      } else {
        if (l.isWarnEnabled()) l.warn("Error in DecisionTreeClassifyBolt");
        throw new RuntimeException("Error when classifying");
      }
    }
  }
}
