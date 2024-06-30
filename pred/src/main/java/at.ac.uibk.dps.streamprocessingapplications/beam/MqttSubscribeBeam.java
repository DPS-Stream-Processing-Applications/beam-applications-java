package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.MqttSubscribeEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.MQTTSubscribeTask;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSubscribeBeam extends DoFn<String, MqttSubscribeEntry> {

  private static long msgid = 1;
  private static Logger l;
  MQTTSubscribeTask mqttSubscribeTask;
  String spoutLogFileName = null;
  Properties p;
  String csvFileNameOutSink; // Full path name of the file at the sink bolt

  public MqttSubscribeBeam() {}

  public MqttSubscribeBeam(Properties p_, String spoutLogFileName) {
    this.csvFileNameOutSink = csvFileNameOutSink;
    p = p_;
    this.spoutLogFileName = spoutLogFileName;
  }

  public MqttSubscribeBeam(Properties p_) {
    p = p_;
  }

  public static void initLogger(Logger l_) {
    l = l_;
  }

  @Setup
  public void setup() throws MqttException {
    mqttSubscribeTask = new MQTTSubscribeTask();
    initLogger(LoggerFactory.getLogger("APP"));
    // mqttSubscribeTask.setup(l, p);
  }

  @ProcessElement
  public void processElement(@Element String input, OutputReceiver<MqttSubscribeEntry> out)
      throws IOException {
    HashMap<String, String> map = new HashMap();
    map.put(AbstractTask.DEFAULT_KEY, "dummy");
    // mqttSubscribeTask.doTask(map);
    String arg1 = (String) mqttSubscribeTask.getLastResult();
    arg1 = "test-12";

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

  @Teardown
  public void cleanUp() {
    mqttSubscribeTask.tearDown();
  }
}
