package at.ac.uibk.dps.streamprocessingapplications.stats.grid;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.beam.sdk.transforms.SerializableBiFunction;

public class KalmanSetter
    implements SerializableBiFunction<GridMeasurement, Double, GridMeasurement> {
  @Override
  public GridMeasurement apply(GridMeasurement measurement, Double tripDistance) {
    measurement.setMeasurement(tripDistance);
    return measurement;
  }

  @Override
  public <V> BiFunction<GridMeasurement, Double, V> andThen(
      Function<? super GridMeasurement, ? extends V> after) {
    return SerializableBiFunction.super.andThen(after);
  }
}
