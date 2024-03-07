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

    private Properties p;

    public DecisionTreeBeam1(Properties p_) {
        p = p_;
    }

    private static Logger l;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    DecisionTreeClassify decisionTreeClassify;

    @Setup
    public void setup() throws MqttException {
        initLogger(LoggerFactory.getLogger("APP"));
        decisionTreeClassify = new DecisionTreeClassify();
        decisionTreeClassify.setup(l, p);
    }

    @ProcessElement
    public void processElement(@Element BlobReadEntry input, OutputReceiver<DecisionTreeEntry> out)
            throws IOException {
        String msgtype = input.getMsgType();
        String analyticsType = input.getAnalyticType();
        String sensorMeta = input.getMeta();

        String obsVal = "10,1955.22,27"; // dummy
        String msgId = "0";

        /* We are getting an model update message so we will update the model only*/

        if (msgtype.equals("modelupdate") && analyticsType.equals("DTC")) {
            byte[] BlobModelObject = (byte[]) input.getBlobModelObject();
            InputStream bytesInputStream = new ByteArrayInputStream(BlobModelObject);
            //        	ByteArrayInputStream BlobModelObject= (ByteArrayInputStream)
            // input.getValueByField("BlobModelObject");
            // do nothing for now
            //            byte[] blobModelObjects = input.getBinaryByField("BlobModelObject");
            //            p.setProperty("CLASSIFICATION.DECISION_TREE.MODEL_PATH",)

            // TODO:  1- Either write model file to local disk - no task code change
            // TODO:  2- Pass it as bytestream , update the code for task
            // TODO:  3- confirm, once this j48tree object will be updated dor not
            try {

                DecisionTreeClassify.j48tree = (J48) SerializationHelper.read(bytesInputStream);
                if (l.isInfoEnabled())
                    l.info("Model is updated DTC {}", DecisionTreeClassify.j48tree);
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

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, obsVal);
        Float res;
        // Float res = decisionTreeClassify.doTask(map);  // index of result-class/enum as return
        //        System.out.println("TestS: DT res " +res);
        res = Float.valueOf("1");
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
