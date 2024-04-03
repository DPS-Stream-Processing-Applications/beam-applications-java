package at.ac.uibk.dps.streamprocessingapplications.etl.taxi.transforms;

import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.model.TaxiRide;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.ZooDefs.OpCode.check;

/**
 * Detects values of the `TaxiRide` class that are out of specified ranges and sets them to null.
 * The ranges used are defined in the
 * <a href="https://github.com/dream-lab/riot-bench/blob/c86414f7f926ed5ae0fab756bb3d82fbfb6e5bf7/modules/tasks/src/main/resources/tasks_TAXI.properties#L31">tasks_TAXI.properties</a>
 * file in the riot-bench repository.
 *
 * <p>
 * The ranges specified in the properties file are not inclusive. For example:
 * </p>
 * <pre>{@code
 * FILTER.RANGE_FILTER.VALID_RANGE=
 * trip_time_in_secs:140:3155,
 * trip_distance:1.37:29.86, // NOTE: Strange, as this is in meters.
 * fare_amount:6.00:201.00,
 * tip_amount:0.65:38.55,
 * tolls_amount:2.50:18.00
 * }</pre>
 */
public class FilterRange extends PTransform<PCollection<TaxiRide>, PCollection<TaxiRide>> {

  @Override
  public PCollection<TaxiRide> expand(PCollection<TaxiRide> input) {
    return input.apply(
        MapElements.into(TypeDescriptor.of(TaxiRide.class))
            .via(
                taxiRide -> {
                    setNullIf(taxiRide::getTripTimeInSecs, FilterRange::isTripTimeOutOfRange, taxiRide::setTripTimeInSecs);
                    setNullIf(taxiRide::getTripDistance, FilterRange::isTripDistanceOutOfRange, taxiRide::setTripDistance);
                    setNullIf(taxiRide::getFareAmount, FilterRange::isFareAmountOutOfRange, taxiRide::setFareAmount);
                    setNullIf(taxiRide::getTipAmount, FilterRange::isTipAmountOutOfRange, taxiRide::setTipAmount);
                    setNullIf(taxiRide::getTollsAmount, FilterRange::isTollsAmountOutOfRange, taxiRide::setTollsAmount);

                    return taxiRide;
                }));
  }

    /**
     * Sets the value to `null` if the condition specified by the predicate is met.
     *
     * @param <T>       The type of the value being evaluated.
     * @param getter    A supplier function providing an optional value.
     * @param condition The predicate condition to be checked.
     * @param setter    A consumer function to set the value to null if the condition is met.
     */
  private static <T> void setNullIf(Supplier<Optional<T>> getter, Predicate<T> condition, Consumer<T> setter) {
      getter.get().ifPresent(value -> {
          if (condition.test(value)) {
              setter.accept(null);
          }
      });
  }

    private static boolean isTripTimeOutOfRange(double tripTimeInSecs) {
        return !(140.0 < tripTimeInSecs && tripTimeInSecs < 3155.0);
    }

    private static boolean isTripDistanceOutOfRange(double tripDistanceInMeters) {
        return !(1.37 < tripDistanceInMeters && tripDistanceInMeters < 29.86);
    }

    private static boolean isFareAmountOutOfRange(double fareAmount) {
        return !(6.0 < fareAmount && fareAmount < 201.0);
    }

    private static boolean isTipAmountOutOfRange(double tipAmount) {
        return !(0.65 < tipAmount && tipAmount < 38.55);
    }

    private static boolean isTollsAmountOutOfRange(double tollsAmount) {
        return !(2.5 < tollsAmount && tollsAmount < 18.0);
    }
}
