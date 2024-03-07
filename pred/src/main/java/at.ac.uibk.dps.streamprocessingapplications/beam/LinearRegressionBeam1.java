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

    @Setup
    public void setup() throws MqttException {
        linearRegressionPredictor = new LinearRegressionPredictor();
        initLogger(LoggerFactory.getLogger("APP"));
        System.out.println(l);
        linearRegressionPredictor.setup(l, p);
    }

    private Properties p;

    public LinearRegressionBeam1(Properties p_) {
        p = p_;
    }

    private static Logger l;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    LinearRegressionPredictor linearRegressionPredictor;

    @ProcessElement
    public void processElement(@Element SenMlEntry input, OutputReceiver<LinearRegressionEntry> out)
            throws IOException {

        String sensorMeta = input.getMeta();
        String msgtype = input.getMsgtype();
        String analyticsType = input.getAnalyticType();

        String obsVal = "10,1955.22,27";
        String msgId = "0";

        if (!msgtype.equals("modelupdate")) {
            obsVal = input.getObsVal();
            msgId = input.getMsgid();

            if (l.isInfoEnabled()) l.info("modelupdate obsVal-" + obsVal);
        }

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, obsVal);
        // Float res; = linearRegressionPredictor.doTask(map);
        Float res = Float.valueOf("1");

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
