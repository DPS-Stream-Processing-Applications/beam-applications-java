package at.ac.uibk.dps.streamprocessingapplications.stats.fit;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class KalmanGetter implements SerializableFunction<FitnessMeasurements, Double> {
  @Override
  public Double apply(FitnessMeasurements measurement) {
    return measurement.getAnkleGyroX().orElse(0.0);
  }
}
