package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class AnnotateEntry implements Serializable {

    @JsonProperty("msgid")
    private String msgid;

    @JsonProperty("annotData")
    private String annotData;

    @JsonProperty("rowKeyEnd")
    private String rowKeyEnd;

    @JsonProperty("dataSetType")
    private String dataSetType;

    public AnnotateEntry(String msgid, String annotData, String rowKeyEnd, String dataSetType) {
        this.msgid = msgid;
        this.annotData = annotData;
        this.rowKeyEnd = rowKeyEnd;
        this.dataSetType = dataSetType;
    }

    public AnnotateEntry() {
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

    public String getDataSetType() {
        return dataSetType;
    }

    public void setDataSetType(String dataSetType) {
        this.dataSetType = dataSetType;
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
