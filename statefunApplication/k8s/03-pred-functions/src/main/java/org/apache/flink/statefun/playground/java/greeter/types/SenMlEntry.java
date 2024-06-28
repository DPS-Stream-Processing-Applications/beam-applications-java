package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class SenMlEntry {

    @JsonProperty("msgid")
    private String msgid;
    @JsonProperty("sensorID")
    private String sensorID;

    @JsonProperty("meta")
    private String meta;

    @JsonProperty("obsType")
    private String obsType;

    @JsonProperty("obsVal")
    private String obsVal;

    @JsonProperty("msgtype")
    private String msgtype;

    @JsonProperty("analyticType")
    private String analyticType;

    @JsonProperty("datasetType")
    private String dataSetType;

    @JsonProperty("arrivalTime")
    private long arrivalTime;

    public SenMlEntry(
            String msgid,
            String sensorID,
            String meta,
            String obsType,
            String obsVal,
            String msgtype,
            String analyticType, String dataSetType) {
        this.msgid = msgid;
        this.sensorID = sensorID;
        this.meta = meta;
        this.obsType = obsType;
        this.obsVal = obsVal;
        this.msgtype = msgtype;
        this.analyticType = analyticType;
        this.dataSetType = dataSetType;
    }

    public SenMlEntry() {
    }

    public String getSensorID() {
        return sensorID;
    }

    public void setSensorID(String sensorID) {
        this.sensorID = sensorID;
    }

    public String getDataSetType() {
        return dataSetType;
    }

    public void setDataSetType(String dataSetType) {
        this.dataSetType = dataSetType;
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    public String getMsgtype() {
        return msgtype;
    }

    public void setMsgtype(String msgtype) {
        this.msgtype = msgtype;
    }

    public String getMeta() {
        return meta;
    }

    public void setMeta(String meta) {
        this.meta = meta;
    }

    public String getObsType() {
        return obsType;
    }

    public void setObsType(String obsType) {
        this.obsType = obsType;
    }

    public String getAnalyticType() {
        return analyticType;
    }

    public void setAnalyticType(String analyticType) {
        this.analyticType = analyticType;
    }

    public String getObsVal() {
        return obsVal;
    }

    public void setObsVal(String obsVal) {
        this.obsVal = obsVal;
    }

    public long getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(long arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    @Override
    public String toString() {
        return "SenMlEntry{"
                + "msgid='"
                + msgid
                + '\''
                + ", sensorID='"
                + sensorID
                + '\''
                + ", meta='"
                + meta
                + '\''
                + ", obsType='"
                + obsType
                + '\''
                + ", ObsVal='"
                + obsVal
                + '\''
                + ", msgtype='"
                + msgtype
                + '\''
                + ", analyticType='"
                + analyticType
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof SenMlEntry)) return false;
        SenMlEntry other = (SenMlEntry) obj;
        return Objects.equals(msgid, other.msgid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgid, sensorID, meta, obsType, obsVal, msgtype, analyticType);
    }

}
