package at.ac.uibk.dps.streamprocessingapplications.etl.taxi;

import static org.junit.jupiter.api.Assertions.*;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.jupiter.api.Test;

class AnnotationFunctionTest {

  @Test
  void apply() {
    TaxiRide validTaxiTrip2 = new TaxiRide();

    SerializableFunction<TaxiRide, TaxiRide> annotate = new AnnotationFunction();

    System.out.println(annotate.apply(validTaxiTrip2));
  }
}
