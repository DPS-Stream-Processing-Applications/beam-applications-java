package at.ac.uibk.dps.streamprocessingapplications.stats.fit;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.DoFn;

public class AveragingFunction extends DoFn<Iterable<FitnessMeasurements>, Double> {
  @ProcessElement
  public void processElement(
      @Element Iterable<FitnessMeasurements> measurements, OutputReceiver<Double> out) {
    out.output(
        StreamSupport.stream(measurements.spliterator(), false)
            .mapToDouble(measurement -> measurement.getAnkleAccelerationX().orElse(0.0))
            .average()
            .orElse(Double.NaN));
  }
}
