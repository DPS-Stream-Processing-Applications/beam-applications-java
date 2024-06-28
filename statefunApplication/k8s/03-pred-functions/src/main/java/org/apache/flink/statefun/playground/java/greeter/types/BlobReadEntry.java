package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;

public final class BlobReadEntry {

    @JsonProperty("blobModelObject")
    private byte[] BlobModelObject;
    @JsonProperty("msgid")
    private String msgid;

    @JsonProperty("msgType")
    private String msgType;

    @JsonProperty("analyticType")
    private String analyticType;

    @JsonProperty("meta")
    private String meta;

    @JsonProperty("datasetType")
    private String dataSetType;

    @JsonProperty("arrivalTime")
    private long arrivalTime;

    public BlobReadEntry(
            byte[] blobModelObject,
            String msgid,
            String msgType,
            String analyticType,
            String meta, String dataSetType) {
        BlobModelObject = blobModelObject;
        this.msgid = msgid;
        this.msgType = msgType;
        this.analyticType = analyticType;
        this.meta = meta;
        this.dataSetType = dataSetType;
    }


    public BlobReadEntry() {
    }

    public byte[] getBlobModelObject() {
        return BlobModelObject;
    }

    public void setBlobModelObject(byte[] blobModelObject) {
        BlobModelObject = blobModelObject;
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getAnalyticType() {
        return analyticType;
    }

    public void setAnalyticType(String analyticType) {
        this.analyticType = analyticType;
    }

    public String getMeta() {
        return meta;
    }

    public void setMeta(String meta) {
        this.meta = meta;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlobReadEntry that = (BlobReadEntry) o;
        return Objects.equals(msgid, that.msgid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgid);
    }

    @Override
    public String toString() {
        return "BlobReadEntry{"
                + "BlobModelObject="
                + Arrays.toString(BlobModelObject)
                + ", msgid='"
                + msgid
                + '\''
                + ", msgType='"
                + msgType
                + '\''
                + ", analyticType='"
                + analyticType
                + '\''
                + ", meta='"
                + meta
                + '\''
                + '}';
    }


}
