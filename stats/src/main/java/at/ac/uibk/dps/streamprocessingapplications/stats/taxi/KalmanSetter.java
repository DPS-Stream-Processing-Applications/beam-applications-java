package at.ac.uibk.dps.streamprocessingapplications.stats.taxi;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.beam.sdk.transforms.SerializableBiFunction;

public class KalmanSetter implements SerializableBiFunction<TaxiRide, Double, TaxiRide> {
  @Override
  public TaxiRide apply(TaxiRide taxiRide, Double tripDistance) {
    taxiRide.setTripDistance(tripDistance);
    return taxiRide;
  }

  @Override
  public <V> BiFunction<TaxiRide, Double, V> andThen(
      Function<? super TaxiRide, ? extends V> after) {
    return SerializableBiFunction.super.andThen(after);
  }
}
