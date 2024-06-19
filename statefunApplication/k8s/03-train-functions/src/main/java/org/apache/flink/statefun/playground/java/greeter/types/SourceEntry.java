package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

public class SourceEntry implements Serializable {

    @JsonProperty("rowString")
    private String rowString;

    @JsonProperty("msgid")
    private String msgid;

    @JsonProperty("rowKeyStart")
    private String rowKeyStart;

    @JsonProperty("rowKeyEnd")
    private String rowKeyEnd;

    @JsonProperty("dataSetType")
    private String dataSetType;

    public SourceEntry(String rowString, String msgid, String rowKeyStart, String rowKeyEnd, String dataSetType) {
        this.rowString = rowString;
        this.msgid = msgid;
        this.rowKeyStart = rowKeyStart;
        this.rowKeyEnd = rowKeyEnd;
        this.dataSetType = dataSetType;
    }

    public SourceEntry() {
    }

    public String getDataSetType() {
        return dataSetType;
    }

    public void setDataSetType(String dataSetType) {
        this.dataSetType = dataSetType;
    }

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