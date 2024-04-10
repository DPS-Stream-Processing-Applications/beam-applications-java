package at.ac.uibk.dps.streamprocessingapplications.etl.taxi;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class AnnotationFunction implements SerializableFunction<TaxiRide, TaxiRide> {
  private static String[] taxiCompanies = {"c2", "c3", "c4"};
  private static String[] driverNames = {"n2", "n3", "n4"};
  private static String[] taxiCities = {"cty2", "cty3", "cty4"};

  @Override
  public TaxiRide apply(TaxiRide input) {
    // NOTE: NO seed needed as these values are not used later.
    Random rand = new Random();
    List<Integer> indices = rand.ints(3).boxed().collect(Collectors.toList());

    input.setTaxiCompany(taxiCompanies[indices.get(0) % taxiCompanies.length]);
    input.setDriverName(driverNames[indices.get(1) % driverNames.length]);
    input.setTaxiCity(taxiCities[indices.get(2) % taxiCities.length]);

    return input;
  }
}
