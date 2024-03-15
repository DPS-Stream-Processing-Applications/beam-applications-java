package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.AverageEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.ErrorEstimateEntry;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class ErrorEstimateBeam2 extends DoFn<AverageEntry, ErrorEstimateEntry> {
    private static Logger l;
    private final String dataSetType;
    private Properties p;
    private String Res = "0";
    private String avgRes = "0";

    public ErrorEstimateBeam2(Properties p_, String dataSetType) {
        p = p_;
        this.dataSetType = dataSetType;
    }

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

        if (analyticsType.equals("AVG")) {
            avgRes = input.getAvGres();
        }

        //        float air_quality= Float.parseFloat(obsVal.split(",")[4]);

        if (l.isInfoEnabled())
            l.info("analyticsType:{},Res:{},avgRes:{}", analyticsType, Res, avgRes);

        if (analyticsType.equals("MLR")) {
            float errval = 0;
            if (dataSetType.equals("TAXI")) {
                float fare_amount = Float.parseFloat((obsVal).split(",")[2]);
                errval = (fare_amount - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
            }
            if (dataSetType.equals("SYS")) {
                float air_quality = Float.parseFloat((input.getObsVal()).split(",")[4]);
                errval = (air_quality - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
            }
            if (dataSetType.equals("FIT")) {
                float fare_amount = Float.parseFloat((input.getObsVal()).split(",")[7]);
                errval = (fare_amount - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
            }
            if (l.isInfoEnabled()) l.info(("errval -" + errval));
            out.output(new ErrorEstimateEntry(sensorMeta, errval, msgId, analyticsType, obsVal));
        }
    }
}
