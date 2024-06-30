package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;
import java.util.Objects;

public class MqttSubscribeEntry implements Serializable {
  private String analyticType;
  private String blobModelPath;
  private String msgid;
  private long arrivalTime;

  public MqttSubscribeEntry(String analyticType, String blobModelPath, String msgid) {
    this.analyticType = analyticType;
    this.blobModelPath = blobModelPath;
    this.msgid = msgid;
  }

  public String getBlobModelPath() {
    return blobModelPath;
  }

  public void setBlobModelPath(String blobModelPath) {
    blobModelPath = blobModelPath;
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

  public long getArrivalTime() {
    return arrivalTime;
  }

  public void setArrivalTime(long arrivalTime) {
    this.arrivalTime = arrivalTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MqttSubscribeEntry that = (MqttSubscribeEntry) o;
    return Objects.equals(msgid, that.msgid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(msgid);
  }

  @Override
  public String toString() {
    return "MqttSubscribeEntry{"
        + "Analytictype='"
        + analyticType
        + '\''
        + ", BlobModelPath='"
        + blobModelPath
        + '\''
        + ", msgid='"
        + msgid
        + '\''
        + '}';
  }
}
