package at.ac.uibk.dps.streamprocessingapplications.etl.model;

import java.time.Instant;
import java.util.Map;
import javax.annotation.Nullable;
import org.json.JSONObject;

public class SenMLRecordDouble extends AbstractSenMLRecord<Double> {

  public SenMLRecordDouble(
      @Nullable String baseName,
      @Nullable String name,
      @Nullable String unit,
      @Nullable Double value,
      @Nullable Instant time) {
    super(baseName, name, unit, value, time);
  }

  @Override
  public String toString() {
    return new JSONObject(
            Map.of(
                "bn",
                this.getBaseName(),
                "n",
                this.getName(),
                "u",
                this.getUnit(),
                "v",
                this.getValue(),
                "t",
                this.getTime()))
        .toString();
  }
}
