package at.ac.uibk.dps.streamprocessingapplications.etl.taxi;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import java.util.Random;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class AnnotationFunction implements SerializableFunction<TaxiRide, TaxiRide> {
  private static final String[] taxiCompanies = {"c2", "c3", "c4"};
  private static final String[] driverNames = {"n2", "n3", "n4"};
  private static final String[] taxiCities = {"cty2", "cty3", "cty4"};

  @Override
  public TaxiRide apply(TaxiRide input) {
    // NOTE: NO seed needed as these values are not used later.
    Random rand = new Random();
    input.setTaxiCompany(taxiCompanies[rand.nextInt(taxiCompanies.length)]);
    input.setDriverName(driverNames[rand.nextInt(driverNames.length)]);
    input.setTaxiCity(taxiCities[rand.nextInt(taxiCities.length)]);

    return input;
  }
}
