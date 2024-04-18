package at.ac.uibk.dps.streamprocessingapplications.stats.taxi;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.DoFn;

public class AveragingFunction extends DoFn<Iterable<TaxiRide>, Double> {
  @ProcessElement
  public void processElement(@Element Iterable<TaxiRide> taxiRides, OutputReceiver<Double> out) {
    out.output(
        StreamSupport.stream(taxiRides.spliterator(), false)
            .mapToDouble(taxiRide -> taxiRide.getTripDistance().orElse(0.0))
            .average()
            .orElse(Double.NaN));
  }
}
