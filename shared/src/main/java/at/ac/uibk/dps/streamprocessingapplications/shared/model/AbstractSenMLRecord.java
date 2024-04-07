package at.ac.uibk.dps.streamprocessingapplications.shared.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

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
 *   <li>exactly one of v / vs
 *   <li>t
 * </ul>
 */
public abstract class AbstractSenMLRecord<T extends Object> implements Serializable {
  private String baseName;
  private String name;
  private String unit;
  private T value;
  private Instant time;

  /* INFO:
   * As specified in <a href="https://datatracker.ietf.org/doc/html/rfc8427#section-4.2">section 4.2</a>
   * a SenML record can only contain **one** or **zero** value labels.
   * This value can be of type `number`, `boolean`, `string` or `data`.
   * The corresponding labels are: "v", "sv", and "bv".
   */
  public AbstractSenMLRecord(String baseName, String name, String unit, T value, Instant time) {
    this.setBaseName(baseName);
    this.setName(name);
    this.setUnit(unit);
    this.setValue(value);
    this.setTime(time);
  }

  public String getBaseName() {
    return baseName;
  }

  public String getName() {
    return name;
  }

  public String getUnit() {
    return unit;
  }

  public T getValue() {
    return this.value;
  }

  public Instant getTime() {
    return time;
  }

  public String getFullName() {
    return this.getBaseName() + this.getName();
  }

  public void setBaseName(String baseName) {
    this.baseName = baseName;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setUnit(String unit) {
    this.unit = unit;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public void setTime(Instant time) {
    this.time = time;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AbstractSenMLRecord)) return false;
    AbstractSenMLRecord<?> that = (AbstractSenMLRecord<?>) o;
    return Objects.equals(getBaseName(), that.getBaseName())
        && Objects.equals(getName(), that.getName())
        && Objects.equals(getUnit(), that.getUnit())
        // && Objects.equals(getValue(), that.getValue())
        && Objects.equals(getTime(), that.getTime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getBaseName(), getName(), getUnit(), getValue(), getTime());
  }

  @Override
  public abstract String toString();
}
