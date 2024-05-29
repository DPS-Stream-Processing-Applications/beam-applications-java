package at.ac.uibk.dps.streamprocessingapplications.etl.grid;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import java.util.Random;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class AnnotationFunction implements SerializableFunction<GridMeasurement, GridMeasurement> {

  private static final String[] names = {"John Doe", "Jane Doe", "Hans Zimmer"};
  private static final String[] ids = {"a", "b", "c"};

  @Override
  public GridMeasurement apply(GridMeasurement input) {

    // NOTE: NO seed needed as these values are not used later.
    Random rand = new Random();
    input.setHouseId(ids[rand.nextInt(names.length)]);
    input.setOwnerFullName(names[rand.nextInt(names.length)]);

    return input;
  }
}
