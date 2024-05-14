package at.ac.uibk.dps.streamprocessingapplications.shared.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SensorData implements Serializable {
  /*
  [{"u":"string","n":"source","sv":"ci4lr75sl000802ypo4qrcjda23"},{"v":"6.1668213","u":"lon","n":"longitude"},{"v":"46.1927629","u":"lat","n":"latitude"},{"v":"8","u":"far","n":"temperature"},{"v":"53.7","u":"per","n":"humidity"},{"v":"0","u":"per","n":"light"},{"v":"411.02","u":"per","n":"dust"},{"v":"140","u":"per","n":"airquality_raw"}]
   */

  private SenMLRecordString source;
  private SenMLRecordDouble longitude;
  private SenMLRecordDouble latitude;
  private SenMLRecordDouble temperature;
  private SenMLRecordDouble humidity;
  private SenMLRecordDouble light;
  private SenMLRecordDouble dust;
  private SenMLRecordDouble airQuality;
  private SenMLRecordString location;
  private SenMLRecordString brand;

  public SensorData() {
    this.source = new SenMLRecordString(null, "source", "string", null, null);
    this.longitude = new SenMLRecordDouble(null, "longitude", "lon", null, null);
    this.longitude = new SenMLRecordDouble(null, "latitude", "lat", null, null);
    this.temperature = new SenMLRecordDouble(null, "temperature", "far", null, null);
    this.humidity = new SenMLRecordDouble(null, "humidity", "%", null, null);
    this.light = new SenMLRecordDouble(null, "light", "%", null, null);
    this.dust = new SenMLRecordDouble(null, "dust", "%", null, null);
    this.airQuality = new SenMLRecordDouble(null, "airquality_raw", "%", null, null);
    this.location = new SenMLRecordString(null, "location", "string", null, null);
    this.brand = new SenMLRecordString(null, "brand", "string", null, null);
  }

  public SensorData(
      SenMLRecordString source,
      SenMLRecordDouble longitude,
      SenMLRecordDouble latitude,
      SenMLRecordDouble temperature,
      SenMLRecordDouble humidity,
      SenMLRecordDouble light,
      SenMLRecordDouble dust,
      SenMLRecordDouble airQuality) {
    this();
    this.source = source;
    this.longitude = longitude;
    this.latitude = latitude;
    this.temperature = temperature;
    this.humidity = humidity;
    this.light = light;
    this.dust = dust;
    this.airQuality = airQuality;
  }

  public Optional<String> getSource() {
    return Optional.ofNullable(this.source.getValue());
  }

  public void setSource(String source) {
    this.source.setValue(source);
  }

  public Optional<Double> getLongitude() {
    return Optional.ofNullable(longitude.getValue());
  }

  public void setLongitude(Double longitude) {
    this.longitude.setValue(longitude);
  }

  public Optional<Double> getLatitude() {
    return Optional.ofNullable(this.latitude.getValue());
  }

  public void setLatitude(Double latitude) {
    this.latitude.setValue(latitude);
  }

  public Optional<Double> getTemperature() {
    return Optional.ofNullable(this.temperature.getValue());
  }

  public void setTemperature(Double temperature) {
    this.temperature.setValue(temperature);
  }

  public Optional<Double> getHumidity() {
    return Optional.ofNullable(this.humidity.getValue());
  }

  public void setHumidity(Double humidity) {
    this.humidity.setValue(humidity);
  }

  public Optional<Double> getLight() {
    return Optional.ofNullable(this.light.getValue());
  }

  public void setLight(Double light) {
    this.light.setValue(light);
  }

  public Optional<Double> getDust() {
    return Optional.ofNullable(this.dust.getValue());
  }

  public void setDust(Double dust) {
    this.dust.setValue(dust);
  }

  public Optional<Double> getAirQuality() {
    return Optional.ofNullable(airQuality.getValue());
  }

  public void setAirQuality(Double airQuality) {
    this.airQuality.setValue(airQuality);
  }

  public Optional<String> getLocation() {
    return Optional.ofNullable(location.getValue());
  }

  public void setLocation(String location) {
    this.location.setValue(location);
  }

  public Optional<String> getBrand() {
    return Optional.ofNullable(this.brand.getValue());
  }

  public void setBrand(String brand) {
    this.brand.setValue(brand);
  }

  @Override
  public String toString() {
    List<String> nonNullFields = new ArrayList<>();

    Optional.ofNullable(this.source).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.longitude).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.latitude).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.temperature).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.humidity).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.light).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.dust).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.airQuality).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.location).ifPresent(value -> nonNullFields.add(value.toString()));
    Optional.ofNullable(this.brand).ifPresent(value -> nonNullFields.add(value.toString()));

    return "[" + String.join(",", nonNullFields) + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SensorData)) return false;
    SensorData that = (SensorData) o;
    return Objects.equals(getSource(), that.getSource())
        && Objects.equals(getLongitude(), that.getLongitude())
        && Objects.equals(getLatitude(), that.getLatitude())
        && Objects.equals(getTemperature(), that.getTemperature())
        && Objects.equals(getHumidity(), that.getHumidity())
        && Objects.equals(getLight(), that.getLight())
        && Objects.equals(getDust(), that.getDust())
        && Objects.equals(getAirQuality(), that.getAirQuality())
        && Objects.equals(getLocation(), that.getLocation())
        && Objects.equals(getBrand(), that.getBrand());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getSource(),
        getLongitude(),
        getLatitude(),
        getTemperature(),
        getHumidity(),
        getLight(),
        getDust(),
        getAirQuality(),
        getLocation(),
        getBrand());
  }
}
