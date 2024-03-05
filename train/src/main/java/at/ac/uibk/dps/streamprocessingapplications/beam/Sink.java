package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.MqttPublishEntry;
import java.util.Random;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sink extends DoFn<MqttPublishEntry, String> {
    private static final Logger LOG = LoggerFactory.getLogger("APP");

    String csvFileNameOutSink; // Full path name of the file at the sink bolt

    public Sink(String csvFileNameOutSink) {
        Random ran = new Random();
        this.csvFileNameOutSink = csvFileNameOutSink;
    }

    @ProcessElement
    public void processElement(@Element MqttPublishEntry input, OutputReceiver<String> out) {
        String msgId = input.getMsgid();
        System.out.println("In Sink " + msgId);
        out.output(msgId);
    }
}