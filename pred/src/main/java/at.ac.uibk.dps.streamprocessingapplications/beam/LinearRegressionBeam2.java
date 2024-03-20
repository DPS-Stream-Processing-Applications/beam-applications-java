package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.BlobReadEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.LinearRegressionEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.LinearRegressionPredictor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.classifiers.functions.LinearRegression;
import weka.core.SerializationHelper;

public class LinearRegressionBeam2 extends DoFn<BlobReadEntry, LinearRegressionEntry> {

    private static Logger l;
    LinearRegressionPredictor linearRegressionPredictor;
    private Properties p;
    private String dataSetType;

    public LinearRegressionBeam2(Properties p_, String dataSetType) {
        p = p_;
        this.dataSetType = dataSetType;
    }

    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Setup
    public void setup() throws MqttException {
        linearRegressionPredictor = new LinearRegressionPredictor();
        initLogger(LoggerFactory.getLogger("APP"));
        linearRegressionPredictor.setup(l, p);
    }

    @ProcessElement
    public void processElement(
            @Element BlobReadEntry input, OutputReceiver<LinearRegressionEntry> out)
            throws IOException {

        String sensorMeta = input.getMeta();
        String msgtype = input.getMsgType();
        String analyticsType = input.getAnalyticType();

        String obsVal = "";
        String msgId = "0";

        if (dataSetType.equals("TAXI") | dataSetType.equals("FIT")) {
            obsVal = "10,1955.22,27";
        }
        if (dataSetType.equals("SYS")) {
            obsVal = "22.7,49.3,0,1955.22,27";
        }

        if (msgtype.equals("modelupdate") && analyticsType.equals("MLR")) {
            byte[] BlobModelObject = (byte[]) input.getBlobModelObject();
            InputStream bytesInputStream = new ByteArrayInputStream(BlobModelObject);
            //            if(l.isInfoEnabled())
            //                l.info("blob model size "+blobModelObject.toString());

            // TODO:  1- Either write model file to local disk - no task code change
            // TODO:  2- Pass it as bytestream , need to update the code for task

            try {
                LinearRegressionPredictor.lr =
                        (LinearRegression) SerializationHelper.read(bytesInputStream);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Error, when reading BlobObject " + e);
            }
            if (this.l.isInfoEnabled()) {
                this.l.info("Model is updated MLR {} ", LinearRegressionPredictor.lr.toString());
            }
        }

        if (!msgtype.equals("modelupdate")) {
            msgId = input.getMsgid();
        }

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, obsVal);

        Float res = linearRegressionPredictor.doTask(map);

        if (l.isInfoEnabled()) l.info("res linearRegressionPredictor-" + res);

        if (res != null) {
            if (res != Float.MIN_VALUE)
                out.output(
                        new LinearRegressionEntry(
                                sensorMeta, obsVal, msgId, res.toString(), "MLR"));
            else {
                if (l.isWarnEnabled()) l.warn("Error in LinearRegressionPredictorBolt");
                throw new RuntimeException("Res is null or float.min");
            }
        }
    }
}
