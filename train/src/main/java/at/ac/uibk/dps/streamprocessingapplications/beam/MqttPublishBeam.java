package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.BlobUploadEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.MqttPublishEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.MQTTPublishTask;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttPublishBeam extends DoFn<BlobUploadEntry, MqttPublishEntry> {

    private Properties p;

    public MqttPublishBeam(Properties p_) {
        p = p_;
    }

    private static Logger l;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    MQTTPublishTask mqttPublishTask;

    @Setup
    public void setup() throws IOException {
        initLogger(LoggerFactory.getLogger("APP"));

        mqttPublishTask = new MQTTPublishTask();

        mqttPublishTask.setup(l, p);
    }

    @Teardown
    public void cleanup() {
        mqttPublishTask.tearDown();
    }

    @ProcessElement
    public void processElement(@Element BlobUploadEntry input, OutputReceiver<MqttPublishEntry> out)
            throws IOException {
        String msgId = input.getMsgid();
        String filename = input.getFileName();

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, filename);
        Float res = mqttPublishTask.doTask(map);
        out.output(new MqttPublishEntry(msgId));
    }
}
