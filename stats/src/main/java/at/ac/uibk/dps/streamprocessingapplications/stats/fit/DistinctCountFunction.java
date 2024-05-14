package at.ac.uibk.dps.streamprocessingapplications.stats.fit;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.DoFn;

public class DistinctCountFunction extends DoFn<Iterable<FitnessMeasurements>, Long> {
  @ProcessElement
  public void processElement(
      @Element Iterable<FitnessMeasurements> taxiRides, OutputReceiver<Long> out) {
    out.output(
        StreamSupport.stream(taxiRides.spliterator(), false)
            .map(FitnessMeasurements::getSubjectId)
            .distinct()
            .count());
  }
}
