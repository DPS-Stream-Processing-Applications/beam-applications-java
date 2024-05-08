package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class MqttSubscribeEntry {


    @JsonProperty("analaytictype")
    private String analaytictype;
    @JsonProperty("blobModelPath")
    private String blobModelPath;

    @JsonProperty("msgid")
    private String msgid;

    @JsonProperty("datasetType")
    private String dataSetType;


    public String getAnalaytictype() {
        return analaytictype;
    }

    public String getBlobModelPath() {
        return blobModelPath;
    }

    public String getMsgid() {
        return msgid;
    }

    public String getDataSetType() {
        return dataSetType;
    }

    public void setDataSetType(String dataSetType) {
        this.dataSetType = dataSetType;
    }

    public MqttSubscribeEntry(String analaytictype, String blobModelPath, String msgid, String dataSetType) {
        this.analaytictype = analaytictype;
        this.blobModelPath = blobModelPath;
        this.msgid = msgid;
        this.dataSetType = dataSetType;
    }

    public MqttSubscribeEntry() {
    }

    @Override
    public String toString() {
        return "MqttSubscribeEntry{" +
                "analaytictype='" + analaytictype + '\'' +
                ", blobModelPath='" + blobModelPath + '\'' +
                ", msgid='" + msgid + '\'' +
                '}';
    }
}
