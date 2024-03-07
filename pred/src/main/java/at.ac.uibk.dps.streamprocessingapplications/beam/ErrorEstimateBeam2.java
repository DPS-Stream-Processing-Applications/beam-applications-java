package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.AverageEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.ErrorEstimateEntry;
import java.io.IOException;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorEstimateBeam2 extends DoFn<AverageEntry, ErrorEstimateEntry> {
    private Properties p;

    private String Res = "0";
    private String avgRes = "0";

    public ErrorEstimateBeam2(Properties p_) {
        p = p_;
    }

    private static Logger l;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Setup
    public void setup() throws MqttException {
        initLogger(LoggerFactory.getLogger("APP"));
    }

    @ProcessElement
    public void processElement(
            @Element AverageEntry input, DoFn.OutputReceiver<ErrorEstimateEntry> out)
            throws IOException {
        String msgId = input.getMsgid();
        String analyticsType = input.getAnalyticType();
        //        String sensorID=input.getStringByField("sensorID");
        String sensorMeta = input.getMeta();
        String obsVal = input.getObsVal();
        obsVal = "23.0,15.3,36.5";
        float fare_amount = Float.parseFloat((obsVal).split(",")[2]);

        if (analyticsType.equals("AVG")) {
            avgRes = input.getAvGres();
        }

        //        float air_quality= Float.parseFloat(obsVal.split(",")[4]);

        if (l.isInfoEnabled())
            l.info("analyticsType:{},Res:{},avgRes:{}", analyticsType, Res, avgRes);

        if (analyticsType.equals("MLR")) {
            float errval = (fare_amount - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
            if (l.isInfoEnabled()) l.info(("errval -" + errval));
            out.output(new ErrorEstimateEntry(sensorMeta, errval, msgId, analyticsType, obsVal));
        }
    }
}
