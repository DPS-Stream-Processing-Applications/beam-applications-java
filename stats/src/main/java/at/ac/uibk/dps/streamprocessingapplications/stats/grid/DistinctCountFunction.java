package at.ac.uibk.dps.streamprocessingapplications.stats.grid;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.stream.StreamSupport;

public class DistinctCountFunction extends DoFn<Iterable<GridMeasurement>, Long> {
  @ProcessElement
  public void processElement(
          @Element Iterable<GridMeasurement> measurements, OutputReceiver<Long> out) {
    out.output(
        StreamSupport.stream(measurements.spliterator(), false)
            .map(GridMeasurement::getId)
            .distinct()
            .count());
  }
}
