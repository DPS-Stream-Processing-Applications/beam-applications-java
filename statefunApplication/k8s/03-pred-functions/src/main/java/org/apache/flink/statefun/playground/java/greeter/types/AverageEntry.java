package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class AverageEntry {

    @JsonProperty("meta")
    private String meta;
    @JsonProperty("sensorId")
    private String sensorId;
    @JsonProperty("obsType")
    private String obsType;

    @JsonProperty("avGres")
    private String avGres;
    @JsonProperty("obsVal")
    private String obsVal;

    @JsonProperty("msgid")
    private String msgid;

    @JsonProperty("analyticType")
    private String analyticType;

    @JsonProperty("datasetType")
    private String dataSetType;

    @JsonProperty("arrivalTime")
    private long arrivalTime;

    public AverageEntry(
            String meta,
            String sensorId,
            String obsType,
            String avGres,
            String obsVal,
            String msgid,
            String analyticType, String dataSetType) {
        this.meta = meta;
        this.sensorId = sensorId;
        this.obsType = obsType;
        this.avGres = avGres;
        this.obsVal = obsVal;
        this.msgid = msgid;
        this.analyticType = analyticType;
        this.dataSetType = dataSetType;
    }

    public AverageEntry() {
    }

    public String getMeta() {
        return meta;
    }

    public void setMeta(String meta) {
        this.meta = meta;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public String getObsType() {
        return obsType;
    }

    public void setObsType(String obsType) {
        this.obsType = obsType;
    }

    public String getAvGres() {
        return avGres;
    }

    public void setAvGres(String avGres) {
        this.avGres = avGres;
    }

    public String getObsVal() {
        return obsVal;
    }

    public void setObsVal(String obsVal) {
        this.obsVal = obsVal;
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    public String getAnalyticType() {
        return analyticType;
    }

    public void setAnalyticType(String analyticType) {
        this.analyticType = analyticType;
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
        AverageEntry that = (AverageEntry) o;
        return Objects.equals(msgid, that.msgid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgid);
    }

    @Override
    public String toString() {
        return "AverageEntry{"
                + "meta='"
                + meta
                + '\''
                + ", sensorId='"
                + sensorId
                + '\''
                + ", obsType='"
                + obsType
                + '\''
                + ", avGres='"
                + avGres
                + '\''
                + ", obsVal='"
                + obsVal
                + '\''
                + ", msgid='"
                + msgid
                + '\''
                + ", analyticType='"
                + analyticType
                + '\''
                + '}';
    }


}
