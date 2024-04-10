package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.MqttSubscribeEntry;
import at.ac.uibk.dps.streamprocessingapplications.kafka.MyKafkaConsumer;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSubscribeBeam extends DoFn<String, MqttSubscribeEntry> {
    private static long msgid = 1;
    private static Logger l; // TODO: Ensure logger is initialized before use
    MyKafkaConsumer myKafkaConsumer;
    String spoutLogFileName = null;
    Properties p;

    String bootStrapServer;
    String topic;

    public KafkaSubscribeBeam(Properties p_, String spoutLogFileName, String server, String topic) {
        // this.csvFileNameOutSink = csvFileNameOutSink;
        p = p_;
        this.spoutLogFileName = spoutLogFileName;
        initLogger(LoggerFactory.getLogger("APP"));
    }

    public KafkaSubscribeBeam(Properties p_, String server, String topic) {
        p = p_;
        this.bootStrapServer = server;
        this.topic = topic;
    }

    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Setup
    public void setup() throws MqttException {
        initLogger(LoggerFactory.getLogger("APP"));
        myKafkaConsumer =
                new MyKafkaConsumer(bootStrapServer, "group-" + UUID.randomUUID(), 1000, topic);
        myKafkaConsumer.setup(l, p);
    }

    @ProcessElement
    public void processElement(@Element String input, OutputReceiver<MqttSubscribeEntry> out)
            throws IOException {
        // TODO Read packet and forward to next bolt
        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, "dummy");
        // mqttSubscribeTask.doTask(map);
        myKafkaConsumer.doTask(map);
        String arg1 = myKafkaConsumer.getLastResult();
        // arg1 = "test-12";

        if (arg1 != null) {
            try {
                msgid++;
                // ba.batchLogwriter(System.nanoTime(),"MSGID," + msgId);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Exception in processElement of MqttBeam " + e);
            }
            if (l.isInfoEnabled()) l.info("arg1 in MQTTSubscribeSpout {}", arg1);
            // System.out.println("arg " + arg1);
            out.output(new MqttSubscribeEntry(arg1.split("-")[1], arg1, Long.toString(msgid)));
        }
    }

    /*
    @Teardown
    public void cleanUp() {
        mqttSubscribeTask.tearDown();
    }

     */

}
