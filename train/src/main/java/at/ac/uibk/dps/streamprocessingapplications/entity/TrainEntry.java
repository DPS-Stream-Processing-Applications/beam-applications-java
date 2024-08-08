package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;

public class TrainEntry implements Serializable {
  private String model;
  private String msgid;
  private String rowKeyEnd;
  private String analyticType;
  private String fileName;
  private long arrivalTime;

  public TrainEntry(
      String model, String msgid, String rowKeyEnd, String analyticType, String fileName) {
    this.model = model;
    this.msgid = msgid;
    this.rowKeyEnd = rowKeyEnd;
    this.analyticType = analyticType;
    this.fileName = fileName;
  }

  public TrainEntry() {}

  public String getModel() {
    return model;
  }

  public void setModel(String model) {
    this.model = model;
  }

  public String getMsgid() {
    return msgid;
  }

  public void setMsgid(String msgid) {
    this.msgid = msgid;
  }

  public String getRowKeyEnd() {
    return rowKeyEnd;
  }

  public void setRowKeyEnd(String rowKeyEnd) {
    this.rowKeyEnd = rowKeyEnd;
  }

  public String getAnalyticType() {
    return analyticType;
  }

  public void setAnalyticType(String analyticType) {
    this.analyticType = analyticType;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public long getArrivalTime() {
    return arrivalTime;
  }

  public void setArrivalTime(long arrivalTime) {
    this.arrivalTime = arrivalTime;
  }

  @Override
  public String toString() {
    return "TrainEntry{"
        + "model='"
        + model
        + '\''
        + ", msgid='"
        + msgid
        + '\''
        + ", rowKeyEnd='"
        + rowKeyEnd
        + '\''
        + ", analyticType='"
        + analyticType
        + '\''
        + ", fileName='"
        + fileName
        + '\''
        + '}';
  }
}
