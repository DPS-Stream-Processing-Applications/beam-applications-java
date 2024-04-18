package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.MqttPublishEntry;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sink extends DoFn<MqttPublishEntry, String> {
    private static final Logger LOG = LoggerFactory.getLogger("APP");

    @ProcessElement
    public void processElement(@Element MqttPublishEntry input, OutputReceiver<String> out) {
        String msgId = input.getMsgid();
        out.output(msgId);
    }
}
