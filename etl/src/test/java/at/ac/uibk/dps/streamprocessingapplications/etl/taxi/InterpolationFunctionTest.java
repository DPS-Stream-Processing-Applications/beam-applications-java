package at.ac.uibk.dps.streamprocessingapplications.etl.taxi;

import static org.junit.jupiter.api.Assertions.*;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.jupiter.api.Test;

class InterpolationFunctionTest {

  @Test
  void apply() {
    TaxiRide validTaxiTrip1 = new TaxiRide();
    validTaxiTrip1.setTripTimeInSecs(140.0);
    validTaxiTrip1.setTripDistance(1.40);
    validTaxiTrip1.setFareAmount(7.0);
    validTaxiTrip1.setTollsAmount(0.7);
    validTaxiTrip1.setTotalAmount(2.6);

    TaxiRide validTaxiTrip2 = new TaxiRide();
    validTaxiTrip2.setTripTimeInSecs(145.0);
    validTaxiTrip2.setTripDistance(3.1);
    validTaxiTrip2.setFareAmount(9.0);
    validTaxiTrip2.setTollsAmount(1.3);
    validTaxiTrip2.setTotalAmount(4.3);

    TaxiRide invalidTaxiTrip = new TaxiRide();

    TaxiRide interpolatedTaxiTrip = new TaxiRide();
    interpolatedTaxiTrip.setTripTimeInSecs(142.5);
    interpolatedTaxiTrip.setTripDistance(2.25);
    interpolatedTaxiTrip.setFareAmount(8.0);
    interpolatedTaxiTrip.setTollsAmount(1.0);
    interpolatedTaxiTrip.setTotalAmount(3.45);

    SerializableFunction<Iterable<TaxiRide>, Iterable<TaxiRide>> interpolator =
        new InterpolationFunction();

    Iterable<TaxiRide> actual =
        interpolator.apply(List.of(validTaxiTrip1, validTaxiTrip2, invalidTaxiTrip));
    assertEquals(actual, List.of(validTaxiTrip1, validTaxiTrip2, interpolatedTaxiTrip));
  }
}
