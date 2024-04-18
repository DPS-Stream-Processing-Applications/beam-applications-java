package at.ac.uibk.dps.streamprocessingapplications.stats.taxi;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.DoFn;

public class DistinctCountFunction extends DoFn<Iterable<TaxiRide>, Long> {
  @ProcessElement
  public void processElement(@Element Iterable<TaxiRide> taxiRides, OutputReceiver<Long> out) {
    out.output(
        StreamSupport.stream(taxiRides.spliterator(), false)
            .map(TaxiRide::getTaxiIdentifier)
            .distinct()
            .count());
  }
}
