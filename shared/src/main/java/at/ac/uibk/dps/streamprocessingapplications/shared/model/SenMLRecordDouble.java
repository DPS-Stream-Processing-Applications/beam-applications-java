package at.ac.uibk.dps.streamprocessingapplications.shared.model;

import java.time.Instant;
import java.util.Optional;
import org.json.JSONObject;

public class SenMLRecordDouble extends AbstractSenMLRecord<Double> {

  public SenMLRecordDouble(String baseName, String name, String unit, Double value, Instant time) {
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

  @Override
  public boolean equals(Object o) {
    SenMLRecordDouble that = (SenMLRecordDouble) o;
    Double thisValue = this.getValue();
    Double thatValue = that.getValue();
    if (thisValue == null && thatValue == null) return true;
    if (thisValue == null || thatValue == null) return false;

    return super.equals(o) && Math.abs(thisValue - thatValue) < 0.0001;
  }
}
