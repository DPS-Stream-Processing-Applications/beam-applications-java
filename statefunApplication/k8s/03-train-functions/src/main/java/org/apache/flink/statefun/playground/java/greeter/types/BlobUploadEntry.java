package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class BlobUploadEntry implements Serializable {
    @JsonProperty("msgid")
    private String msgid;

    @JsonProperty("fileName")
    private String fileName;

    @JsonProperty("dataSetType")
    private String dataSetType;

    public BlobUploadEntry(String msgid, String fileName, String dataSetType) {
        this.msgid = msgid;
        this.fileName = fileName;
        this.dataSetType = dataSetType;
    }

    public BlobUploadEntry() {
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

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "BlobUploadEntry{" +
                "msgid='" + msgid + '\'' +
                ", fileName='" + fileName + '\'' +
                ", dataSetType='" + dataSetType + '\'' +
                '}';
    }
}
