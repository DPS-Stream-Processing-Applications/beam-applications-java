package at.ac.uibk.dps.streamprocessingapplications.etl.fit;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import java.util.Random;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class AnnotationFunction
    implements SerializableFunction<FitnessMeasurements, FitnessMeasurements> {

  private static final String[] names = {"John", "Jane", "Hans"};
  private static final double[] ages = {2, 5, 60};

  @Override
  public FitnessMeasurements apply(FitnessMeasurements input) {

    // NOTE: NO seed needed as these values are not used later.
    Random rand = new Random();
    input.setName(names[rand.nextInt(names.length)]);
    input.setAge(ages[rand.nextInt(names.length)]);

    return input;
  }
}
