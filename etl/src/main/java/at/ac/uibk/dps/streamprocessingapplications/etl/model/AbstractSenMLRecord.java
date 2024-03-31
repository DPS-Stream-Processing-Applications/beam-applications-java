package at.ac.uibk.dps.streamprocessingapplications.etl.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import javax.annotation.Nullable;
import org.json.JSONObject;

/**
 * WARN: This is not a full implementation of the SenML specification it only implements a subset!
 * <br>
 * <br>
 * This class represents a SenML sensor value formed of a subset of the official <a
 * href="https://datatracker.ietf.org/doc/html/rfc8428">specification</a><br>
 * The following SenML labels are supported by this implementation:
 *
 * <ul>
 *   <li>bn
 *   <li>n
 *   <li>u
 *   <li>v / sv / bv
 *   <li>t
 * </ul>
 */
public abstract class SenMLRecord<T> implements Serializable {
  @Nullable private String baseName;

  @Nullable private String name;
  @Nullable private String unit;
  @Nullable private T value;
  @Nullable private Instant time;

  public SenMLRecord(String senMLString) {
    JSONObject record = new JSONObject(senMLString);
    this.baseName = record.has("bn") ? record.optString("bn") : null;
    this.name = record.has("n") ? record.optString("n") : null;
    this.unit = record.has("u") ? record.optString("u") : null;

    /* INFO:
     * As specified in <a href="https://datatracker.ietf.org/doc/html/rfc8427#section-4.2">section 4.2</a>
     * a SenML record can only contain **one** or **zero** value labels.
     * This value can be of type `number`, `boolean`, `string` or `data`.
     * The corresponding labels are: "v", "sv", and "bv".
     */
    // Set<String> supportedValueLabels = Set.of("v", "vb", "vs", "vd");
    // this.value =
    // record.keySet().stream()
    // .filter(supportedValueLabels::contains)
    // .map(record::optString)
    // .findFirst()
    //  .orElse(null);

    this.time =
        record.has("t") ? Instant.ofEpochSecond(Long.parseLong(record.optString("t"))) : null;
  }

  public SenMLRecord(@Nullable String baseName, @Nullable String name, @Nullable String unit, @Nullable T value, @Nullable Instant time) {
    this.baseName = baseName;
    this.name = name;
    this.unit = unit;
    this.value = value;
    this.time = time;
  }

  @Nullable public String getBaseName() {
    return baseName;
  }

  @Nullable public String getName() {
    return name;
  }

  @Nullable public String getUnit() {
    return unit;
  }

  @Nullable public T getValue() {
    return this.value;
  };

  @Nullable public Instant getTime() {
    return time;
  }

  public String getFullName() {
    return this.getBaseName() + this.getName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SenMLRecord)) return false;
    SenMLRecord<?> that = (SenMLRecord<?>) o;
    return Objects.equals(getBaseName(), that.getBaseName())
        && Objects.equals(getName(), that.getName())
        && Objects.equals(getUnit(), that.getUnit())
        && Objects.equals(getValue(), that.getValue())
        && Objects.equals(getTime(), that.getTime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getBaseName(), getName(), getUnit(), getValue(), getTime());
  }
}
