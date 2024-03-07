package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.ErrorEstimateEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.MqttPublishEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.MQTTPublishTask;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttPublishBeam1 extends DoFn<ErrorEstimateEntry, MqttPublishEntry> {

    private Properties p;

    public MqttPublishBeam1(Properties p_) {
        p = p_;
    }

    private static Logger l;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    MQTTPublishTask mqttPublishTask;

    @Setup
    public void setup() throws MqttException {
        initLogger(LoggerFactory.getLogger("APP"));
        mqttPublishTask = new MQTTPublishTask();
        mqttPublishTask.setup(l, p);
    }

    @ProcessElement
    public void processElement(
            @Element ErrorEstimateEntry input, DoFn.OutputReceiver<MqttPublishEntry> out)
            throws IOException {
        String msgId = (String) input.getMsgid();
        String meta = (String) input.getMeta();
        //    	String obsType = (String)input.getValueByField("OBSTYPE");
        String analyticsType = input.getAnalyticType();
        String obsVal = input.getObsval();

        StringBuffer temp = new StringBuffer();
        String res = "";
        if (analyticsType.equals("MLR")) {
            res = String.valueOf(input.getError());
            temp.append(msgId)
                    .append(",")
                    .append(meta)
                    .append(",")
                    .append(analyticsType)
                    .append(",obsVal:")
                    .append(obsVal)
                    .append(",ERROR:")
                    .append(res);
        }

        if (l.isInfoEnabled()) l.info("MQTT result:{}", temp);

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, String.valueOf(temp));
        mqttPublishTask.doTask(map);

        out.output(new MqttPublishEntry(msgId, meta, obsVal));
    }

    @Teardown
    public void clearUp() {
        mqttPublishTask.tearDown();
    }
}
