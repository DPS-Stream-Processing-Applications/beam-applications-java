package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;
import java.util.Objects;

public class ErrorEstimateEntry implements Serializable, MqttPublishInput {
  private String meta;
  private float error;
  private String msgid;
  private String analyticType;
  private String obsval;

  public ErrorEstimateEntry(
      String meta, float error, String msgid, String analyticType, String obsval) {
    this.meta = meta;
    this.error = error;
    this.msgid = msgid;
    this.analyticType = analyticType;
    this.obsval = obsval;
  }

  public String getMeta() {
    return meta;
  }

  public void setMeta(String meta) {
    this.meta = meta;
  }

  public float getError() {
    return error;
  }

  public void setError(float error) {
    this.error = error;
  }

  public String getMsgid() {
    return msgid;
  }

  public void setMsgid(String msgid) {
    this.msgid = msgid;
  }

  public String getAnalyticType() {
    return analyticType;
  }

  public void setAnalyticType(String analyticType) {
    this.analyticType = analyticType;
  }

  public String getObsval() {
    return obsval;
  }

  @Override
  public String getRes() {
    return String.valueOf(error);
  }

  public void setObsval(String obsval) {
    this.obsval = obsval;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ErrorEstimateEntry that = (ErrorEstimateEntry) o;
    return Objects.equals(msgid, that.msgid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(msgid);
  }

  @Override
  public String toString() {
    return "ErrorEstimateEntry{"
        + "meta='"
        + meta
        + '\''
        + ", error='"
        + error
        + '\''
        + ", msgid='"
        + msgid
        + '\''
        + ", analyticType='"
        + analyticType
        + '\''
        + ", obsval='"
        + obsval
        + '\''
        + '}';
  }
}
