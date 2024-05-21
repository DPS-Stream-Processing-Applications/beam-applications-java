package at.ac.uibk.dps.streamprocessingapplications.stats.grid;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class KalmanGetter implements SerializableFunction<GridMeasurement, Double> {
  @Override
  public Double apply(GridMeasurement measurement) {
    return measurement.getMeasurement().orElse(0.0);
  }
}
