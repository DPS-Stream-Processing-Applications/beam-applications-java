package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class TrainEntry implements Serializable {

    @JsonProperty("model")
    private String model;
    @JsonProperty("msgid")
    private String msgid;
    @JsonProperty("rowKeyEnd")
    private String rowKeyEnd;
    @JsonProperty("analyticType")
    private String analyticType;
    @JsonProperty("fileName")
    private String fileName;

    @JsonProperty("dataSetType")
    private String dataSetType;

    @JsonProperty("arrivalTime")
    private long arrivalTime;

    public TrainEntry(
            String model, String msgid, String rowKeyEnd, String analyticType, String fileName, String dataSetType) {
        this.model = model;
        this.msgid = msgid;
        this.rowKeyEnd = rowKeyEnd;
        this.analyticType = analyticType;
        this.fileName = fileName;
        this.dataSetType = dataSetType;
    }

    public TrainEntry() {
    }

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

    public String getDataSetType() {
        return dataSetType;
    }

    public void setDataSetType(String dataSetType) {
        this.dataSetType = dataSetType;
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
