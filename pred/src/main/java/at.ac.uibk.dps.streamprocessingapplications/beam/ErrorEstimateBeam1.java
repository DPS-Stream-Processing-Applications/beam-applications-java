package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.ErrorEstimateEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.LinearRegressionEntry;
import java.io.IOException;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorEstimateBeam1 extends DoFn<LinearRegressionEntry, ErrorEstimateEntry> {

    private Properties p;

    private String Res = "0";
    private String avgRes = "0";

    private final String dataSetType;

    public ErrorEstimateBeam1(Properties p_, String dataSetType) {
        p = p_;
        this.dataSetType = dataSetType;
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
            @Element LinearRegressionEntry input, DoFn.OutputReceiver<ErrorEstimateEntry> out)
            throws IOException {
        String msgId = input.getMsgid();
        String analyticsType = input.getAnalyticType();
        //        String sensorID=input.getStringByField("sensorID");
        String sensorMeta = input.getMeta();
        String obsVal = input.getObsval();
        // obsVal = "13.0,15.3,76.3";
        if (analyticsType.equals("MLR")) {
            Res = input.getRes();
        }

        if (l.isInfoEnabled())
            l.info("analyticsType:{},Res:{},avgRes:{}", analyticsType, Res, avgRes);

        if (analyticsType.equals("MLR")) {
            float errval = 0;
            String fareString;
            String[] obsValSplit = obsVal.split(",");
            if (dataSetType.equals("TAXI")) {
                if (obsValSplit.length > 3) {
                    fareString = obsValSplit[10];
                } else {
                    fareString = obsValSplit[2];
                }

                float fare_amount = Float.parseFloat(fareString);
                errval = (fare_amount - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
            }
            if (dataSetType.equals("SYS")) {
                System.out.println("SYS-input: " + input.getObsval());
                float air_quality = Float.parseFloat((input.getObsval()).split(",")[4]);
                errval = (air_quality - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
            }

            if (l.isInfoEnabled()) l.info(("errval -" + errval));
            out.output(new ErrorEstimateEntry(sensorMeta, errval, msgId, analyticsType, obsVal));
        }
    }
}
