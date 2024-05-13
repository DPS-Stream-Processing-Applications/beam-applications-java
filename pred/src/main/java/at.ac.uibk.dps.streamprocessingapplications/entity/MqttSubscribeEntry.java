package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;
import java.util.Objects;

public class MqttSubscribeEntry implements Serializable {
  private String Analaytictype;
  private String BlobModelPath;
  private String msgid;

  public MqttSubscribeEntry(String analaytictype, String blobModelPath, String msgid) {
    Analaytictype = analaytictype;
    BlobModelPath = blobModelPath;
    this.msgid = msgid;
  }

  public String getBlobModelPath() {
    return BlobModelPath;
  }

  public void setBlobModelPath(String blobModelPath) {
    BlobModelPath = blobModelPath;
  }

  public String getMsgid() {
    return msgid;
  }

  public void setMsgid(String msgid) {
    this.msgid = msgid;
  }

  public String getAnalaytictype() {
    return Analaytictype;
  }

  public void setAnalaytictype(String analaytictype) {
    Analaytictype = analaytictype;
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
        + Analaytictype
        + '\''
        + ", BlobModelPath='"
        + BlobModelPath
        + '\''
        + ", msgid='"
        + msgid
        + '\''
        + '}';
  }
}
