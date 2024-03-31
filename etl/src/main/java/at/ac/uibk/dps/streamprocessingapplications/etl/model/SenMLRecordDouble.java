package at.ac.uibk.dps.streamprocessingapplications.etl.model;

import java.time.Instant;
import javax.annotation.Nullable;

public class SenMLRecordDouble extends AbstractSenMLRecord<Double> {

  public SenMLRecordDouble(
      @Nullable String baseName,
      @Nullable String name,
      @Nullable String unit,
      @Nullable Double value,
      @Nullable Instant time) {
    super(baseName, name, unit, value, time);
  }
}
