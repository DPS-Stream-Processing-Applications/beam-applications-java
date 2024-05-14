package at.ac.uibk.dps.streamprocessingapplications.shared.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class FitnessMeasurements implements Serializable {

  private SenMLRecordString subjectId;
  private SenMLRecordDouble chestAccelerationX;
  private SenMLRecordDouble chestAccelerationY;
  private SenMLRecordDouble chestAccelerationZ;
  private SenMLRecordDouble ECGLead1;
  private SenMLRecordDouble ECGLead2;
  private SenMLRecordDouble ankleAccelerationX;
  private SenMLRecordDouble ankleAccelerationY;
  private SenMLRecordDouble ankleAccelerationZ;
  private SenMLRecordDouble ankleGyroX;
  private SenMLRecordDouble ankleGyroY;
  private SenMLRecordDouble ankleGyroZ;
  private SenMLRecordDouble ankleMagnetometerX;
  private SenMLRecordDouble ankleMagnetometerY;
  private SenMLRecordDouble ankleMagnetometerZ;
  private SenMLRecordDouble armAccelerationX;
  private SenMLRecordDouble armAccelerationY;
  private SenMLRecordDouble armAccelerationZ;
  private SenMLRecordDouble armGyroX;
  private SenMLRecordDouble armGyroY;
  private SenMLRecordDouble armGyroZ;
  private SenMLRecordDouble armMagnetometerX;
  private SenMLRecordDouble armMagnetometerY;
  private SenMLRecordDouble armMagnetometerZ;
  private SenMLRecordDouble label;
  private SenMLRecordDouble age;
  private SenMLRecordString name;

  public FitnessMeasurements() {
    this.subjectId = new SenMLRecordString(null, "subjectId", "string", null, null);
    this.chestAccelerationX = new SenMLRecordDouble(null, "acc_chest_x", "m/s2", null, null);
    this.chestAccelerationY = new SenMLRecordDouble(null, "acc_chest_y", "m/s2", null, null);
    this.chestAccelerationZ = new SenMLRecordDouble(null, "acc_chest_z", "m/s2", null, null);
    this.ECGLead1 = new SenMLRecordDouble(null, "ecg_lead_1", "mV", null, null);
    this.ECGLead2 = new SenMLRecordDouble(null, "ecg_lead_2", "mV", null, null);
    this.ankleAccelerationX = new SenMLRecordDouble(null, "acc_ankle_x", "m/s2", null, null);
    this.ankleAccelerationY = new SenMLRecordDouble(null, "acc_ankle_y", "m/s2", null, null);
    this.ankleAccelerationZ = new SenMLRecordDouble(null, "acc_ankle_z", "m/s2", null, null);
    this.ankleGyroX = new SenMLRecordDouble(null, "gyro_ankle_x", "deg/s2", null, null);
    this.ankleGyroY = new SenMLRecordDouble(null, "gyro_ankle_y", "deg/s2", null, null);
    this.ankleGyroZ = new SenMLRecordDouble(null, "gyro_ankle_z", "deg/s2", null, null);
    this.ankleMagnetometerX = new SenMLRecordDouble(null, "magnetometer_ankle_x", "T", null, null);
    this.ankleMagnetometerY = new SenMLRecordDouble(null, "magnetometer_ankle_y", "T", null, null);
    this.ankleMagnetometerZ = new SenMLRecordDouble(null, "magnetometer_ankle_z", "T", null, null);
    this.armAccelerationX = new SenMLRecordDouble(null, "acc_arm_x", "m/s2", null, null);
    this.armAccelerationY = new SenMLRecordDouble(null, "acc_arm_y", "m/s2", null, null);
    this.armAccelerationZ = new SenMLRecordDouble(null, "acc_arm_z", "m/s2", null, null);
    this.armGyroX = new SenMLRecordDouble(null, "gyro_arm_x", "deg/s2", null, null);
    this.armGyroY = new SenMLRecordDouble(null, "gyro_arm_y", "deg/s2", null, null);
    this.armGyroZ = new SenMLRecordDouble(null, "gyro_arm_z", "deg/s2", null, null);
    this.armMagnetometerX = new SenMLRecordDouble(null, "magnetometer_arm_x", "T", null, null);
    this.armMagnetometerY = new SenMLRecordDouble(null, "magnetometer_arm_y", "T", null, null);
    this.armMagnetometerZ = new SenMLRecordDouble(null, "magnetometer_arm_z", "T", null, null);
    this.label = new SenMLRecordDouble(null, "label", "number", null, null);
    this.age = new SenMLRecordDouble(null, "age", "number", null, null);
    this.name = new SenMLRecordString(null, "name", "string", null, null);
  }

  public FitnessMeasurements(
      SenMLRecordString subjectId,
      SenMLRecordDouble chestAccelerationX,
      SenMLRecordDouble chestAccelerationY,
      SenMLRecordDouble chestAccelerationZ,
      SenMLRecordDouble ECGLead1,
      SenMLRecordDouble ECGLead2,
      SenMLRecordDouble ankleAccelerationX,
      SenMLRecordDouble ankleAccelerationY,
      SenMLRecordDouble ankleAccelerationZ,
      SenMLRecordDouble ankleGyroX,
      SenMLRecordDouble ankleGyroY,
      SenMLRecordDouble ankleGyroZ,
      SenMLRecordDouble ankleMagnetometerX,
      SenMLRecordDouble ankleMagnetometerY,
      SenMLRecordDouble ankleMagnetometerZ,
      SenMLRecordDouble armAccelerationX,
      SenMLRecordDouble armAccelerationY,
      SenMLRecordDouble armAccelerationZ,
      SenMLRecordDouble armGyroX,
      SenMLRecordDouble armGyroY,
      SenMLRecordDouble armGyroZ,
      SenMLRecordDouble armMagnetometerX,
      SenMLRecordDouble armMagnetometerY,
      SenMLRecordDouble armMagnetometerZ,
      SenMLRecordDouble label) {
    this();
    this.subjectId = subjectId;
    this.chestAccelerationX = chestAccelerationX;
    this.chestAccelerationY = chestAccelerationY;
    this.chestAccelerationZ = chestAccelerationZ;
    this.ECGLead1 = ECGLead1;
    this.ECGLead2 = ECGLead2;
    this.ankleAccelerationX = ankleAccelerationX;
    this.ankleAccelerationY = ankleAccelerationY;
    this.ankleAccelerationZ = ankleAccelerationZ;
    this.ankleGyroX = ankleGyroX;
    this.ankleGyroY = ankleGyroY;
    this.ankleGyroZ = ankleGyroZ;
    this.ankleMagnetometerX = ankleMagnetometerX;
    this.ankleMagnetometerY = ankleMagnetometerY;
    this.ankleMagnetometerZ = ankleMagnetometerZ;
    this.armAccelerationX = armAccelerationX;
    this.armAccelerationY = armAccelerationY;
    this.armAccelerationZ = armAccelerationZ;
    this.armGyroX = armGyroX;
    this.armGyroY = armGyroY;
    this.armGyroZ = armGyroZ;
    this.armMagnetometerX = armMagnetometerX;
    this.armMagnetometerY = armMagnetometerY;
    this.armMagnetometerZ = armMagnetometerZ;
    this.label = label;
  }

  public Optional<String> getSubjectId() {
    return Optional.ofNullable(this.subjectId.getValue());
  }

  public void setSubjectId(String subjectId) {
    this.subjectId.setValue(subjectId);
  }

  public Optional<Double> getChestAccelerationX() {
    return Optional.ofNullable(this.chestAccelerationX.getValue());
  }

  public void setChestAccelerationX(Double chestAccelerationX) {
    this.chestAccelerationX.setValue(chestAccelerationX);
  }

  public Optional<Double> getChestAccelerationY() {
    return Optional.ofNullable(this.chestAccelerationY.getValue());
  }

  public void setChestAccelerationY(Double chestAccelerationY) {
    this.chestAccelerationY.setValue(chestAccelerationY);
  }

  public Optional<Double> getChestAccelerationZ() {
    return Optional.ofNullable(this.chestAccelerationZ.getValue());
  }

  public void setChestAccelerationZ(Double chestAccelerationZ) {
    this.chestAccelerationZ.setValue(chestAccelerationZ);
  }

  public Optional<Double> getECGLead1() {
    return Optional.ofNullable(this.ECGLead1.getValue());
  }

  public void setECGLead1(Double ECGLead1) {
    this.ECGLead1.setValue(ECGLead1);
  }

  public Optional<Double> getECGLead2() {
    return Optional.ofNullable(this.ECGLead2.getValue());
  }

  public void setECGLead2(Double ECGLead2) {
    this.ECGLead2.setValue(ECGLead2);
  }

  public Optional<Double> getAnkleAccelerationX() {
    return Optional.ofNullable(this.ankleAccelerationX.getValue());
  }

  public void setAnkleAccelerationX(Double ankleAccelerationX) {
    this.ankleAccelerationX.setValue(ankleAccelerationX);
  }

  public Optional<Double> getAnkleAccelerationY() {
    return Optional.ofNullable(this.ankleAccelerationY.getValue());
  }

  public void setAnkleAccelerationY(Double ankleAccelerationY) {
    this.ankleAccelerationY.setValue(ankleAccelerationY);
  }

  public Optional<Double> getAnkleAccelerationZ() {
    return Optional.ofNullable(this.ankleAccelerationZ.getValue());
  }

  public void setAnkleAccelerationZ(Double ankleAccelerationZ) {
    this.ankleAccelerationZ.setValue(ankleAccelerationZ);
  }

  public Optional<Double> getAnkleGyroX() {
    return Optional.ofNullable(this.ankleGyroX.getValue());
  }

  public void setAnkleGyroX(Double ankleGyroX) {
    this.ankleGyroX.setValue(ankleGyroX);
  }

  public Optional<Double> getAnkleGyroY() {
    return Optional.ofNullable(this.ankleGyroY.getValue());
  }

  public void setAnkleGyroY(Double ankleGyroY) {
    this.ankleGyroY.setValue(ankleGyroY);
  }

  public Optional<Double> getAnkleGyroZ() {
    return Optional.ofNullable(this.ankleGyroZ.getValue());
  }

  public void setAnkleGyroZ(Double ankleGyroZ) {
    this.ankleGyroZ.setValue(ankleGyroZ);
  }

  public Optional<Double> getAnkleMagnetometerX() {
    return Optional.ofNullable(this.ankleMagnetometerX.getValue());
  }

  public void setAnkleMagnetometerX(Double ankleMagnetometerX) {
    this.ankleMagnetometerX.setValue(ankleMagnetometerX);
  }

  public Optional<Double> getAnkleMagnetometerY() {
    return Optional.ofNullable(this.ankleMagnetometerY.getValue());
  }

  public void setAnkleMagnetometerY(Double ankleMagnetometerY) {
    this.ankleMagnetometerY.setValue(ankleMagnetometerY);
  }

  public Optional<Double> getAnkleMagnetometerZ() {
    return Optional.ofNullable(this.ankleMagnetometerZ.getValue());
  }

  public void setAnkleMagnetometerZ(Double ankleMagnetometerZ) {
    this.ankleMagnetometerZ.setValue(ankleMagnetometerZ);
  }

  public Optional<Double> getArmAccelerationX() {
    return Optional.ofNullable(this.armAccelerationX.getValue());
  }

  public void setArmAccelerationX(Double armAccelerationX) {
    this.armAccelerationX.setValue(armAccelerationX);
  }

  public Optional<Double> getArmAccelerationY() {
    return Optional.ofNullable(this.armAccelerationY.getValue());
  }

  public void setArmAccelerationY(Double armAccelerationY) {
    this.armAccelerationY.setValue(armAccelerationY);
  }

  public Optional<Double> getArmAccelerationZ() {
    return Optional.ofNullable(this.armAccelerationZ.getValue());
  }

  public void setArmAccelerationZ(Double armAccelerationZ) {
    this.armAccelerationZ.setValue(armAccelerationZ);
  }

  public Optional<Double> getArmGyroX() {
    return Optional.ofNullable(this.armGyroX.getValue());
  }

  public void setArmGyroX(Double armGyroX) {
    this.armGyroX.setValue(armGyroX);
  }

  public Optional<Double> getArmGyroY() {
    return Optional.ofNullable(this.armGyroY.getValue());
  }

  public void setArmGyroY(Double armGyroY) {
    this.armGyroY.setValue(armGyroY);
  }

  public Optional<Double> getArmGyroZ() {
    return Optional.ofNullable(this.armGyroZ.getValue());
  }

  public void setArmGyroZ(Double armGyroZ) {
    this.armGyroZ.setValue(armGyroZ);
  }

  public Optional<Double> getArmMagnetometerX() {
    return Optional.ofNullable(this.armMagnetometerX.getValue());
  }

  public void setArmMagnetometerX(Double armMagnetometerX) {
    this.armMagnetometerX.setValue(armMagnetometerX);
  }

  public Optional<Double> getArmMagnetometerY() {
    return Optional.ofNullable(this.armMagnetometerY.getValue());
  }

  public void setArmMagnetometerY(Double armMagnetometerY) {
    this.armMagnetometerY.setValue(armMagnetometerY);
  }

  public Optional<Double> getArmMagnetometerZ() {
    return Optional.ofNullable(this.armMagnetometerZ.getValue());
  }

  public void setArmMagnetometerZ(Double armMagnetometerZ) {
    this.armMagnetometerZ.setValue(armMagnetometerZ);
  }

  public Optional<Double> getLabel() {
    return Optional.ofNullable(this.label.getValue());
  }

  public void setLabel(Double label) {
    this.label.setValue(label);
  }

  public Optional<Double> getAge() {
    return Optional.ofNullable(this.age.getValue());
  }

  public void setAge(Double age) {
    this.age.setValue(age);
  }

  public Optional<String> getName() {
    return Optional.ofNullable(this.name.getValue());
  }

  public void setName(String name) {
    this.name.setValue(name);
  }
  @Override
  public String toString() {
    List<String> nonNullFields = new ArrayList<>();

    Optional.ofNullable(this.subjectId).ifPresent(value -> nonNullFields.add(value.toString()));

    Optional.ofNullable(this.chestAccelerationX)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.chestAccelerationY)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.chestAccelerationZ)
        .ifPresent(value -> nonNullFields.add(value.toString()));

    Optional.ofNullable(this.ECGLead1).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.ECGLead2).ifPresent(value -> nonNullFields.add(value.toString()));

    Optional.ofNullable(this.ankleAccelerationX)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.ankleAccelerationY)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.ankleAccelerationZ)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.ankleGyroX).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.ankleGyroY).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.ankleGyroZ).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.ankleMagnetometerX)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.ankleMagnetometerY)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.ankleMagnetometerZ)
        .ifPresent(value -> nonNullFields.add(value.toString()));

    Optional.ofNullable(this.armAccelerationX)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.armAccelerationY)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.armAccelerationZ)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.armGyroX).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.armGyroY).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.armGyroZ).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.armMagnetometerX)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.armMagnetometerY)
        .ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.armMagnetometerZ)
        .ifPresent(value -> nonNullFields.add(value.toString()));

    Optional.ofNullable(this.label).ifPresent(value -> nonNullFields.add(value.toString()));

    return "[" + String.join(",", nonNullFields) + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FitnessMeasurements)) return false;
    FitnessMeasurements that = (FitnessMeasurements) o;
    return Objects.equals(getSubjectId(), that.getSubjectId())
        && Objects.equals(getChestAccelerationX(), that.getChestAccelerationX())
        && Objects.equals(getChestAccelerationY(), that.getChestAccelerationY())
        && Objects.equals(getChestAccelerationZ(), that.getChestAccelerationZ())
        && Objects.equals(getECGLead1(), that.getECGLead1())
        && Objects.equals(getECGLead2(), that.getECGLead2())
        && Objects.equals(getAnkleAccelerationX(), that.getAnkleAccelerationX())
        && Objects.equals(getAnkleAccelerationY(), that.getAnkleAccelerationY())
        && Objects.equals(getAnkleAccelerationZ(), that.getAnkleAccelerationZ())
        && Objects.equals(getAnkleGyroX(), that.getAnkleGyroX())
        && Objects.equals(getAnkleGyroY(), that.getAnkleGyroY())
        && Objects.equals(getAnkleGyroZ(), that.getAnkleGyroZ())
        && Objects.equals(getAnkleMagnetometerX(), that.getAnkleMagnetometerX())
        && Objects.equals(getAnkleMagnetometerY(), that.getAnkleMagnetometerY())
        && Objects.equals(getAnkleMagnetometerZ(), that.getAnkleMagnetometerZ())
        && Objects.equals(getArmAccelerationX(), that.getArmAccelerationX())
        && Objects.equals(getArmAccelerationY(), that.getArmAccelerationY())
        && Objects.equals(getArmAccelerationZ(), that.getArmAccelerationZ())
        && Objects.equals(getArmGyroX(), that.getArmGyroX())
        && Objects.equals(getArmGyroY(), that.getArmGyroY())
        && Objects.equals(getArmGyroZ(), that.getArmGyroZ())
        && Objects.equals(getArmMagnetometerX(), that.getArmMagnetometerX())
        && Objects.equals(getArmMagnetometerY(), that.getArmMagnetometerY())
        && Objects.equals(getArmMagnetometerZ(), that.getArmMagnetometerZ())
        && Objects.equals(getLabel(), that.getLabel());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getSubjectId(),
        getChestAccelerationX(),
        getChestAccelerationY(),
        getChestAccelerationZ(),
        getECGLead1(),
        getECGLead2(),
        getAnkleAccelerationX(),
        getAnkleAccelerationY(),
        getAnkleAccelerationZ(),
        getAnkleGyroX(),
        getAnkleGyroY(),
        getAnkleGyroZ(),
        getAnkleMagnetometerX(),
        getAnkleMagnetometerY(),
        getAnkleMagnetometerZ(),
        getArmAccelerationX(),
        getArmAccelerationY(),
        getArmAccelerationZ(),
        getArmGyroX(),
        getArmGyroY(),
        getArmGyroZ(),
        getArmMagnetometerX(),
        getArmMagnetometerY(),
        getArmMagnetometerZ(),
        getLabel());
  }
}
