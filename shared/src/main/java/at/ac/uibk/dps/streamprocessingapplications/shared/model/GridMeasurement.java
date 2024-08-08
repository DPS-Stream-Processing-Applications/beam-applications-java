package at.ac.uibk.dps.streamprocessingapplications.shared.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class GridMeasurement implements Serializable {
  // [{"n":  "id",  "u":  "double",  "v":  19503.0},
  // {"n":  "grid_measurement",  "u":  "double",  "v":  0.14},
  // {"n":  "timestamp",  "u":  "s",  "v":  1392.0}]
  private SenMLRecordDouble id;
  private SenMLRecordDouble measurement;
  private SenMLRecordDouble timestamp;
  private SenMLRecordString houseId;
  private SenMLRecordString ownerFullName;

  public GridMeasurement() {
    this.id = new SenMLRecordDouble(null, "id", "double", null, null);
    this.measurement = new SenMLRecordDouble(null, "measurement", "double", null, null);
    this.timestamp = new SenMLRecordDouble(null, "timestamp", "double", null, null);
    this.houseId = new SenMLRecordString(null, "house_id", "string", null, null);
    this.ownerFullName = new SenMLRecordString(null, "ownerFullName", "string", null, null);
  }

  public GridMeasurement(
      SenMLRecordDouble id, SenMLRecordDouble measurement, SenMLRecordDouble timestamp) {
    this();
    this.id = id;
    this.measurement = measurement;
    this.timestamp = timestamp;
  }

  public Optional<Double> getId() {
    return Optional.ofNullable(this.id.getValue());
  }

  public void setId(Double id) {
    this.id.setValue(id);
  }

  public Optional<Double> getMeasurement() {
    return Optional.ofNullable(this.measurement.getValue());
  }

  public void setMeasurement(Double measurement) {
    this.measurement.setValue(measurement);
  }

  public Optional<Double> getTimestamp() {
    return Optional.ofNullable(this.timestamp.getValue());
  }

  public void setTimestamp(Double timestamp) {
    this.timestamp.setValue(timestamp);
  }

  public Optional<String> getHouseId() {
    return Optional.ofNullable(houseId.getValue());
  }

  public void setHouseId(String houseId) {
    this.houseId.setValue(houseId);
  }

  public Optional<String> getOwnerFullName() {
    return Optional.ofNullable(ownerFullName.getValue());
  }

  public void setOwnerFullName(String ownerFullName) {
    this.ownerFullName.setValue(ownerFullName);
  }

  @Override
  public String toString() {
    List<String> nonNullFields = new ArrayList<>();

    Optional.ofNullable(this.id).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.measurement).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.timestamp).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.houseId).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.ownerFullName).ifPresent(value -> nonNullFields.add(value.toString()));

    return "[" + String.join(",", nonNullFields) + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GridMeasurement)) return false;
    GridMeasurement that = (GridMeasurement) o;
    return Objects.equals(getId(), that.getId())
        && Objects.equals(getMeasurement(), that.getMeasurement())
        && Objects.equals(getTimestamp(), that.getTimestamp())
        && Objects.equals(getHouseId(), that.getHouseId())
        && Objects.equals(getOwnerFullName(), that.getOwnerFullName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getId(), getMeasurement(), getTimestamp(), getHouseId(), getOwnerFullName());
  }
}
