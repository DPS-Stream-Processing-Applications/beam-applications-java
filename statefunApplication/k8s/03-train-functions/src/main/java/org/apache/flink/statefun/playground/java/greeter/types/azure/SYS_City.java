package org.apache.flink.statefun.playground.java.greeter.types.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class SYS_City extends TableServiceEntity {
  private String ts,
      source,
      longitude,
      latitude,
      temperature,
      humidity,
      light,
      dust,
      airquality_raw;
  private long rangeKey;

  public SYS_City() {}

  public long getRangeKey() {
    return rangeKey;
  }

  public void setRangeKey(long rangeKey) {
    this.rangeKey = rangeKey;
  }

  public String getTs() {
    return ts;
  }

  public void setTs(String timestamp) {
    this.ts = timestamp;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getLongitude() {
    return longitude;
  }

  public void setLongitude(String longitude) {
    this.longitude = longitude;
  }

  public String getLatitude() {
    return latitude;
  }

  public void setLatitude(String latitude) {
    this.latitude = latitude;
  }

  public String getTemperature() {
    return temperature;
  }

  public void setTemperature(String temperature) {
    this.temperature = temperature;
  }

  public String getHumidity() {
    return humidity;
  }

  public void setHumidity(String humidity) {
    this.humidity = humidity;
  }

  public String getLight() {
    return light;
  }

  public void setLight(String light) {
    this.light = light;
  }

  public String getDust() {
    return dust;
  }

  public void setDust(String dust) {
    this.dust = dust;
  }

  public String getAirquality_raw() {
    return airquality_raw;
  }

  public void setAirquality_raw(String airquality_raw) {
    this.airquality_raw = airquality_raw;
  }
}
