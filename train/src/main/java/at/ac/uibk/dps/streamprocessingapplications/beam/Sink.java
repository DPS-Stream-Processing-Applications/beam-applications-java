package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.MqttPublishEntry;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sink extends DoFn<MqttPublishEntry, String> {
  private static final Logger LOG = LoggerFactory.getLogger("APP");
  private final Gauge gauge;
  String csvFileNameOutSink;

  public Sink(String csvFileNameOutSink) {
    this.csvFileNameOutSink = csvFileNameOutSink;
    this.gauge = Metrics.gauge(Sink.class, "Custom_End_to_End_Latency");
  }

  @ProcessElement
  public void processElement(@Element MqttPublishEntry input, OutputReceiver<String> out) {
    String msgId = input.getMsgid();
    LOG.info("In Sink " + msgId);
    long latency = System.currentTimeMillis() - input.getArrivalTime();
    gauge.set(latency);
    out.output(msgId);
  }
}
