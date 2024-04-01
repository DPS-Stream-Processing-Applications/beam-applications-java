package at.ac.uibk.dps.streamprocessingapplications.etl.model;

import java.time.Instant;
import java.util.Optional;
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
    JSONObject jsonObject = new JSONObject();

    Optional.ofNullable(getBaseName()).ifPresent(val -> jsonObject.put("bn", val));
    Optional.ofNullable(getName()).ifPresent(val -> jsonObject.put("n", val));
    Optional.ofNullable(getUnit()).ifPresent(val -> jsonObject.put("u", val));
    Optional.ofNullable(getValue()).ifPresent(val -> jsonObject.put("v", val));
    Optional.ofNullable(getTime()).ifPresent(val -> jsonObject.put("t", val));

    return jsonObject.toString();
  }
}
