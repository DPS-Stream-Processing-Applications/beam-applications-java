package at.ac.uibk.dps.streamprocessingapplications.beam;

import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sink extends DoFn<Long, Long> {
  private static final Logger LOG = LoggerFactory.getLogger(Sink.class);

  private final Distribution distribution;

  public Sink() {
    this.distribution = Metrics.distribution(Sink.class, "custom_latency");
  }

  @ProcessElement
  public void processElement(@Element Long input, OutputReceiver<Long> out) {
    long latency = System.currentTimeMillis() - input;
    distribution.update(latency);
    out.output(input);
  }
}
