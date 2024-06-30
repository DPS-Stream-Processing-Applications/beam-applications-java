package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;

public class AnnotateEntry implements Serializable {
  private String msgid;
  private String annotData;
  private String rowKeyEnd;
  private long arrivalTime;

  public AnnotateEntry(String msgid, String annotData, String rowKeyEnd) {
    this.msgid = msgid;
    this.annotData = annotData;
    this.rowKeyEnd = rowKeyEnd;
  }

  public String getMsgid() {
    return msgid;
  }

  public void setMsgid(String msgid) {
    this.msgid = msgid;
  }

  public String getAnnotData() {
    return annotData;
  }

  public void setAnnotData(String annotData) {
    this.annotData = annotData;
  }

  public String getRowKeyEnd() {
    return rowKeyEnd;
  }

  public void setRowKeyEnd(String rowKeyEnd) {
    this.rowKeyEnd = rowKeyEnd;
  }

  public long getArrivalTime() {
    return arrivalTime;
  }

  public void setArrivalTime(long arrivalTime) {
    this.arrivalTime = arrivalTime;
  }

  @Override
  public String toString() {
    return "AnnotateEntry{"
        + "msgid='"
        + msgid
        + '\''
        + ", annotData='"
        + annotData
        + '\''
        + ", rowKeyEnd='"
        + rowKeyEnd
        + '\''
        + '}';
  }
}
