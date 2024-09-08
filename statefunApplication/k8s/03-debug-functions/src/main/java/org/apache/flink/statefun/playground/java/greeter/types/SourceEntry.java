package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class SourceEntry {
    @JsonProperty("msgid")
    private long msgid;

    @JsonProperty("payload")
    private String payload;


    @JsonProperty("datasetType")
    private String dataSetType;

    @JsonProperty("arrivalTime")
    private long arrivalTime;

    public SourceEntry() {
    }

    public SourceEntry(long msgid, String payload, String dataSetType) {
        this.msgid = msgid;
        this.payload = payload;
        this.dataSetType = dataSetType;
    }

    public long getMsgid() {
        return msgid;
    }

    public String getPayload() {
        return payload;
    }

    public String getDataSetType() {
        return dataSetType;
    }

    public void setDataSetType(String dataSetType) {
        this.dataSetType = dataSetType;
    }

    public long getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(long arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    @Override
    public String toString() {
        return "SourceEntry{" +
                "msgid=" + msgid +
                ", payload='" + payload + '\'' +
                '}';
    }
}

