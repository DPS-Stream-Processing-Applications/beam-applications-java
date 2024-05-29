package at.ac.uibk.dps.streamprocessingapplications.etl.fit;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class InterpolationFunction
    implements SerializableFunction<Iterable<FitnessMeasurements>, Iterable<FitnessMeasurements>> {
  private double meanAnkleAccelerationX;
  private double meanAnkleAccelerationY;
  private double meanAnkleAccelerationZ;

  @Override
  public Iterable<FitnessMeasurements> apply(Iterable<FitnessMeasurements> input) {
    this.calculateMeans(input);
    return StreamSupport.stream(input.spliterator(), false)
        .map(this::interpolate)
        .collect(Collectors.toList());
  }

  public InterpolationFunction() {
    meanAnkleAccelerationX = 0;
    meanAnkleAccelerationY = 0;
    meanAnkleAccelerationZ = 0;
  }

  private FitnessMeasurements interpolate(FitnessMeasurements measurements) {
    if (measurements.getAnkleAccelerationX().isEmpty()) {
      measurements.setAnkleAccelerationX(this.meanAnkleAccelerationX);
    }
    if (measurements.getAnkleAccelerationY().isEmpty()) {
      measurements.setAnkleAccelerationY(this.meanAnkleAccelerationY);
    }
    if (measurements.getAnkleAccelerationZ().isEmpty()) {
      measurements.setAnkleAccelerationZ(this.meanAnkleAccelerationZ);
    }
    return measurements;
  }

  private void calculateMeans(Iterable<FitnessMeasurements> measurements) {
    Collection<Double> validAnkleAccelerationsX = new ArrayList<>();
    Collection<Double> validAnkleAccelerationsY = new ArrayList<>();
    Collection<Double> validAnkleAccelerationsZ = new ArrayList<>();

    for (FitnessMeasurements measurement : measurements) {
      measurement.getAnkleAccelerationX().ifPresent(validAnkleAccelerationsX::add);
      measurement.getAnkleAccelerationY().ifPresent(validAnkleAccelerationsY::add);
      measurement.getAnkleAccelerationZ().ifPresent(validAnkleAccelerationsZ::add);
    }

    this.meanAnkleAccelerationX =
        validAnkleAccelerationsX.stream().mapToDouble(d -> d).average().orElse(0.0);
    this.meanAnkleAccelerationY =
        validAnkleAccelerationsY.stream().mapToDouble(d -> d).average().orElse(0.0);
    this.meanAnkleAccelerationZ =
        validAnkleAccelerationsZ.stream().mapToDouble(d -> d).average().orElse(0.0);
  }
}
