package at.ac.uibk.dps.streamprocessingapplications.etl.model;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.json.JSONObject;

public class SenMLRecordString extends AbstractSenMLRecord<String> {
  public SenMLRecordString(String baseName, String name, String unit, String value, Instant time) {
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

  @Override
  public boolean equals(Object o) {
    SenMLRecordString that = (SenMLRecordString) o;
    String thisValue = this.getValue();
    String thatValue = that.getValue();
    if (thisValue == null && thatValue == null) return true;
    if (thisValue == null || thatValue == null) return false;

    return super.equals(o) && Objects.equals(thisValue, thatValue);
  }
}
