package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;

public class DbEntry implements Serializable {
  private String mgsid;
  private String trainData;
  private String rowKeyEnd;
  private long arrivalTime;

  public String getMgsid() {
    return mgsid;
  }

  public void setMgsid(String mgsid) {
    this.mgsid = mgsid;
  }

  public String getTrainData() {
    return trainData;
  }

  public void setTrainData(String trainData) {
    this.trainData = trainData;
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
    return "DbEntry{"
        + "mgsid='"
        + mgsid
        + '\''
        + ", trainData='"
        + trainData
        + '\''
        + ", rowKeyEnd='"
        + rowKeyEnd
        + '\''
        + '}';
  }
}
