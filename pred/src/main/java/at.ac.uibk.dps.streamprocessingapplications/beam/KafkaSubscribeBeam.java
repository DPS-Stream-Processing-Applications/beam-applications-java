package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.MqttSubscribeEntry;
import java.io.IOException;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSubscribeBeam extends DoFn<String, MqttSubscribeEntry> {
  private static long msgid = 1;
  private static Logger l;

  String spoutLogFileName = null;
  Properties p;

  public KafkaSubscribeBeam(Properties p_, String spoutLogFileName) {
    p = p_;
    this.spoutLogFileName = spoutLogFileName;
    initLogger(LoggerFactory.getLogger("APP"));
  }

  public KafkaSubscribeBeam(Properties p_) {
    p = p_;
  }

  public static void initLogger(Logger l_) {
    l = l_;
  }

  @Setup
  public void setup() throws MqttException {
    initLogger(LoggerFactory.getLogger("APP"));
  }

  @ProcessElement
  public void processElement(@Element String input, OutputReceiver<MqttSubscribeEntry> out)
      throws IOException {
    String arg1 = input;

    if (arg1 != null) {
      try {
        msgid++;
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Exception in processElement of MqttBeam " + e);
      }
      if (l.isInfoEnabled()) l.info("arg1 in MQTTSubscribeSpout {}", arg1);
      out.output(new MqttSubscribeEntry(arg1.split("-")[1], arg1, Long.toString(msgid)));
    }
  }
}
