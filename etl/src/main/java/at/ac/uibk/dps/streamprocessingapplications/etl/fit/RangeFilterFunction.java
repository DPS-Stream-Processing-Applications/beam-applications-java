package at.ac.uibk.dps.streamprocessingapplications.etl.fit;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class RangeFilterFunction
    implements SerializableFunction<FitnessMeasurements, FitnessMeasurements> {

  public FitnessMeasurements apply(FitnessMeasurements measurements) {
    setNullIf(
        measurements::getAnkleAccelerationX,
        RangeFilterFunction::isAnkleAccelerationXOutOfRange,
        measurements::setAnkleAccelerationX);
    setNullIf(
        measurements::getAnkleAccelerationY,
        RangeFilterFunction::isAnkleAccelerationYOutOfRange,
        measurements::setAnkleAccelerationY);
    setNullIf(
        measurements::getAnkleAccelerationZ,
        RangeFilterFunction::isAnkleAccelerationZOutOfRange,
        measurements::setAnkleAccelerationZ);

    return measurements;
  }

  /**
   * Sets the value to `null` if the condition specified by the predicate is met.
   *
   * @param <T> The type of the value being evaluated.
   * @param getter A supplier function providing an optional value.
   * @param condition The predicate condition to be checked.
   * @param setter A consumer function to set the value to null if the condition is met.
   */
  private static <T> void setNullIf(
      Supplier<Optional<T>> getter, Predicate<T> condition, Consumer<T> setter) {
    getter
        .get()
        .ifPresent(
            value -> {
              if (condition.test(value)) {
                setter.accept(null);
              }
            });
  }

  private static boolean isAnkleAccelerationXOutOfRange(Double acceleration) {
    return !(-2.0 <= acceleration && acceleration <= 30.0);
  }

  private static boolean isAnkleAccelerationYOutOfRange(Double acceleration) {
    return !(-2.0 <= acceleration && acceleration <= 30.0);
  }

  private static boolean isAnkleAccelerationZOutOfRange(Double acceleration) {
    return !(-2.0 <= acceleration && acceleration <= 30.0);
  }
}
