package at.ac.uibk.dps.streamprocessingapplications.beam;

import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sink extends DoFn<Long, Long> {
  private static final Logger LOG = LoggerFactory.getLogger("APP");
  private final Gauge gauge;

  public Sink() {
    this.gauge = Metrics.gauge(Sink.class, "custom_latency");
  }

  @ProcessElement
  public void processElement(@Element Long input, OutputReceiver<Long> out) {
    if (input != 0L) {
      long latency = System.currentTimeMillis() - input;
      gauge.set(latency);
    }
    out.output(input);
  }
}
