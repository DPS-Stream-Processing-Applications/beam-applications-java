package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.BlobUploadEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.MqttPublishEntry;
import at.ac.uibk.dps.streamprocessingapplications.kafka.MyKafkaProducer;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPublishBeam extends DoFn<BlobUploadEntry, MqttPublishEntry> {
  private static Logger l;
  MyKafkaProducer myKafkaProducer;
  String server;
  String topic;
  private Properties p;

  public KafkaPublishBeam(Properties p_, String server, String topic) {
    p = p_;
    this.server = server;
    this.topic = topic;
  }

  public static void initLogger(Logger l_) {
    l = l_;
  }

  @Setup
  public void setup() throws MqttException {
    initLogger(LoggerFactory.getLogger("APP"));
    myKafkaProducer = new MyKafkaProducer(server, topic, p);
    myKafkaProducer.setup(l, p);
  }

  @ProcessElement
  public void processElement(
      @Element BlobUploadEntry input, DoFn.OutputReceiver<MqttPublishEntry> out)
      throws IOException {
    String msgId = input.getMsgid();
    String filename = input.getFileName();

    HashMap<String, String> map = new HashMap();
    map.put(AbstractTask.DEFAULT_KEY, filename);
    Float res = 93f;
    if (l.isInfoEnabled()) l.info("MQTT result:{}", res);
    myKafkaProducer.doTask(map);
    out.output(new MqttPublishEntry(msgId));
  }
}
