package at.ac.uibk.dps.streamprocessingapplications.stats.grid;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.DoFn;

public class AveragingFunction extends DoFn<Iterable<GridMeasurement>, Double> {
  @ProcessElement
  public void processElement(
      @Element Iterable<GridMeasurement> measurements, OutputReceiver<Double> out) {
    out.output(
        StreamSupport.stream(measurements.spliterator(), false)
            .mapToDouble(measurement -> measurement.getMeasurement().orElse(0.0))
            .average()
            .orElse(Double.NaN));
  }
}
