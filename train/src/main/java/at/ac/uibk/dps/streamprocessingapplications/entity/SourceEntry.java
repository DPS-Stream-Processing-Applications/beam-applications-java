package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;
import java.util.Objects;

public class SourceEntry implements Serializable {
  private String rowString;
  private String msgid;
  private String rowKeyStart;
  private String rowKeyEnd;
  private long arrivalTime;

  public SourceEntry(String rowString, String msgid, String rowKeyStart, String rowKeyEnd) {
    this.rowString = rowString;
    this.msgid = msgid;
    this.rowKeyStart = rowKeyStart;
    this.rowKeyEnd = rowKeyEnd;
  }

  public SourceEntry() {}

  public String getRowString() {
    return rowString;
  }

  public void setRowString(String rowString) {
    this.rowString = rowString;
  }

  public String getMsgid() {
    return msgid;
  }

  public void setMsgid(String msgid) {
    this.msgid = msgid;
  }

  public String getRowKeyStart() {
    return rowKeyStart;
  }

  public void setRowKeyStart(String rowKeyStart) {
    this.rowKeyStart = rowKeyStart;
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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SourceEntry that = (SourceEntry) o;
    return Objects.equals(msgid, that.msgid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(msgid);
  }

  @Override
  public String toString() {
    return "SourceEntry{"
        + "rowString='"
        + rowString
        + '\''
        + ", msgid='"
        + msgid
        + '\''
        + ", rowKeyStart='"
        + rowKeyStart
        + '\''
        + ", rowKeyEnd='"
        + rowKeyEnd
        + '\''
        + '}';
  }
}
