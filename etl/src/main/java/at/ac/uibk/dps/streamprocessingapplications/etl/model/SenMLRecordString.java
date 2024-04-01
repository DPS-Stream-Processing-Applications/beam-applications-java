package at.ac.uibk.dps.streamprocessingapplications.etl.model;

import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nullable;
import org.json.JSONObject;

public class SenMLRecordString extends AbstractSenMLRecord<String> {
  public SenMLRecordString(
      @Nullable String baseName,
      @Nullable String name,
      @Nullable String unit,
      @Nullable String value,
      @Nullable Instant time) {
    super(baseName, name, unit, value, time);
  }

    @Override
    public String toString() {
        JSONObject jsonObject = new JSONObject();

        Optional.ofNullable(getBaseName()).ifPresent(val -> jsonObject.put("bn", val));
        Optional.ofNullable(getName()).ifPresent(val -> jsonObject.put("n", val));
        Optional.ofNullable(getUnit()).ifPresent(val -> jsonObject.put("u", val));
        Optional.ofNullable(getValue()).ifPresent(val -> jsonObject.put("vs", val));
        Optional.ofNullable(getTime()).ifPresent(val -> jsonObject.put("t", val));

        return jsonObject.toString();
    }
}
