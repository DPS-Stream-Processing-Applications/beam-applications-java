package at.ac.uibk.dps.streamprocessingapplications.stats.fit;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.beam.sdk.transforms.SerializableBiFunction;

public class KalmanSetter
    implements SerializableBiFunction<FitnessMeasurements, Double, FitnessMeasurements> {
  @Override
  public FitnessMeasurements apply(FitnessMeasurements measurement, Double tripDistance) {
    measurement.setAnkleGyroX(tripDistance);
    return measurement;
  }

  @Override
  public <V> BiFunction<FitnessMeasurements, Double, V> andThen(
      Function<? super FitnessMeasurements, ? extends V> after) {
    return SerializableBiFunction.super.andThen(after);
  }
}
