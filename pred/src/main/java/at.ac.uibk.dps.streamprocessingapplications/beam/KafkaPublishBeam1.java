package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.ErrorEstimateEntry;
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

public class KafkaPublishBeam1 extends DoFn<ErrorEstimateEntry, MqttPublishEntry> {
  private static Logger l;
  MyKafkaProducer myKafkaProducer;
  String server;
  String topic;
  private Properties p;

  public KafkaPublishBeam1(Properties p_, String server, String topic) {
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
    myKafkaProducer.doTask(map);
    out.output(new MqttPublishEntry(msgId, meta, obsVal));
  }
}
