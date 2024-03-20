package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.DecisionTreeEntry;
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

public class MqttPublishBeam2 extends DoFn<DecisionTreeEntry, MqttPublishEntry> {

    private static Logger l;
    MQTTPublishTask mqttPublishTask;
    private Properties p;

    public MqttPublishBeam2(Properties p_) {
        p = p_;
    }

    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Setup
    public void setup() throws MqttException {
        initLogger(LoggerFactory.getLogger("APP"));
        mqttPublishTask = new MQTTPublishTask();
        mqttPublishTask.setup(l, p);
    }

    @ProcessElement
    public void processElement(
            @Element DecisionTreeEntry input, DoFn.OutputReceiver<MqttPublishEntry> out)
            throws IOException {
        String msgId = (String) input.getMsgid();
        String meta = (String) input.getMeta();
        //    	String obsType = (String)input.getValueByField("OBSTYPE");
        String analyticsType = input.getAnalyticType();
        String obsVal = input.getObsval();

        StringBuffer temp = new StringBuffer();
        String res = "";
        if (analyticsType.equals("DTC")) {
            res = input.getRes();
            temp.append(msgId)
                    .append(",")
                    .append(meta)
                    .append(",")
                    .append(analyticsType)
                    .append(",obsVal:")
                    .append(obsVal)
                    .append(",RES:")
                    .append(res);
        }

        if (l.isInfoEnabled()) l.info("MQTT result:{}", temp);

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, String.valueOf(temp));
        mqttPublishTask.doTask(map);

        out.output(new MqttPublishEntry(msgId, meta, obsVal));
    }

    @Teardown
    public void cleanUp() {
        mqttPublishTask.tearDown();
    }
}
