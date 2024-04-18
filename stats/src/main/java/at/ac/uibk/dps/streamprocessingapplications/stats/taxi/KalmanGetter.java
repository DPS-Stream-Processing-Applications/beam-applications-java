package at.ac.uibk.dps.streamprocessingapplications.stats.taxi;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class KalmanGetter implements SerializableFunction<TaxiRide, Double> {
  @Override
  public Double apply(TaxiRide ride) {
    return ride.getTripDistance().orElse(0.0);
  }
}
