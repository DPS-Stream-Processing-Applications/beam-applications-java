package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class BlobReadEntry implements Serializable {

  private byte[] BlobModelObject;
  private String msgid;
  private String msgType;
  private String analyticType;
  private String meta;
  private long arrivalTime;

  public BlobReadEntry(
      byte[] blobModelObject, String msgid, String msgType, String analyticType, String meta) {
    BlobModelObject = blobModelObject;
    this.msgid = msgid;
    this.msgType = msgType;
    this.analyticType = analyticType;
    this.meta = meta;
  }

  public byte[] getBlobModelObject() {
    return BlobModelObject;
  }

  public void setBlobModelObject(byte[] blobModelObject) {
    BlobModelObject = blobModelObject;
  }

  public String getMsgid() {
    return msgid;
  }

  public void setMsgid(String msgid) {
    this.msgid = msgid;
  }

  public String getMsgType() {
    return msgType;
  }

  public void setMsgType(String msgType) {
    this.msgType = msgType;
  }

  public String getAnalyticType() {
    return analyticType;
  }

  public void setAnalyticType(String analyticType) {
    this.analyticType = analyticType;
  }

  public String getMeta() {
    return meta;
  }

  public void setMeta(String meta) {
    this.meta = meta;
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
    BlobReadEntry that = (BlobReadEntry) o;
    return Objects.equals(msgid, that.msgid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(msgid);
  }

  @Override
  public String toString() {
    return "BlobReadEntry{"
        + "BlobModelObject="
        + Arrays.toString(BlobModelObject)
        + ", msgid='"
        + msgid
        + '\''
        + ", msgType='"
        + msgType
        + '\''
        + ", analyticType='"
        + analyticType
        + '\''
        + ", meta='"
        + meta
        + '\''
        + '}';
  }
}
