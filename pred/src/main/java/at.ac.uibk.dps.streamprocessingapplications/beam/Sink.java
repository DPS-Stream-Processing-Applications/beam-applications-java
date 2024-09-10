package at.ac.uibk.dps.streamprocessingapplications.beam;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sink extends DoFn<String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(Sink.class);

  @ProcessElement
  public void processElement(@Element String input, OutputReceiver<String> out) {
    LOG.info("In sink: " + input);
    out.output(input);
  }
}
