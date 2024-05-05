package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class SourceEntry {
    @JsonProperty("msgid")
    private long msgid;

    @JsonProperty("payload")
    private String payload;


    @JsonProperty("datasetType")
    private String dataSetType;

    public SourceEntry() {}

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

    @Override
    public String toString() {
        return "SourceEntry{" +
                "msgid=" + msgid +
                ", payload='" + payload + '\'' +
                '}';
    }
}

