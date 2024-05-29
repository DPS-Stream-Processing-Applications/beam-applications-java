package at.ac.uibk.dps.streamprocessingapplications.etl.grid;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class RangeFilterFunction implements SerializableFunction<GridMeasurement, GridMeasurement> {

  public GridMeasurement apply(GridMeasurement measurement) {
    setNullIf(
        measurement::getMeasurement,
        RangeFilterFunction::isMeasurementOutOfRange,
        measurement::setMeasurement);
    return measurement;
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

  private static boolean isMeasurementOutOfRange(Double measurement) {
    return !(0.08 <= measurement && measurement <= 0.95);
  }
}
