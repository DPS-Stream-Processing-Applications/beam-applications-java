package at.ac.uibk.dps.streamprocessingapplications.etl.grid;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class InterpolationFunction
    implements SerializableFunction<Iterable<GridMeasurement>, Iterable<GridMeasurement>> {
  private double meanMeasurement;

  @Override
  public Iterable<GridMeasurement> apply(Iterable<GridMeasurement> input) {
    this.calculateMeans(input);
    return StreamSupport.stream(input.spliterator(), false)
        .map(this::interpolate)
        .collect(Collectors.toList());
  }

  public InterpolationFunction() {
    meanMeasurement = 0;
  }

  private GridMeasurement interpolate(GridMeasurement measurements) {
    if (measurements.getMeasurement().isEmpty()) {
      measurements.setMeasurement(this.meanMeasurement);
    }
    return measurements;
  }

  private void calculateMeans(Iterable<GridMeasurement> measurements) {
    Collection<Double> validMeasurements = new ArrayList<>();

    for (GridMeasurement measurement : measurements) {
      measurement.getMeasurement().ifPresent(validMeasurements::add);
    }

    this.meanMeasurement = validMeasurements.stream().mapToDouble(d -> d).average().orElse(0.0);
  }
}
