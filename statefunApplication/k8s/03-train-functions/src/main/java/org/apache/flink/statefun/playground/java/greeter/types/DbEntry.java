package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class DbEntry implements Serializable {
  @JsonProperty("msgid")
  private String mgsid;
  @JsonProperty("trainData")
  private String trainData;
  @JsonProperty("rowKeyEnd")
  private String rowKeyEnd;

  @JsonProperty("dataSetType")
  private String dataSetType;

  public DbEntry(String mgsid, String trainData, String rowKeyEnd, String dataSetType) {
    this.mgsid = mgsid;
    this.trainData = trainData;
    this.rowKeyEnd = rowKeyEnd;
    this.dataSetType = dataSetType;
  }

  public DbEntry() {
  }

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

  public String getDataSetType() {
    return dataSetType;
  }

  public void setDataSetType(String dataSetType) {
    this.dataSetType = dataSetType;
  }

  public void setRowKeyEnd(String rowKeyEnd) {
    this.rowKeyEnd = rowKeyEnd;
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
