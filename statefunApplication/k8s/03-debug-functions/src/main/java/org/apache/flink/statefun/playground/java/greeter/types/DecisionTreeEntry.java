package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class DecisionTreeEntry {
    @JsonProperty("meta")
    private String meta;
    @JsonProperty("obsval")
    private String obsval;

    @JsonProperty("msgid")
    private String msgid;

    @JsonProperty("res")
    private String res;

    @JsonProperty("analyticType")
    private String analyticType;

    @JsonProperty("datasetType")
    private String dataSetType;

    @JsonProperty("arrivalTime")
    private long arrivalTime;

    public DecisionTreeEntry(
            String meta, String obsval, String msgid, String res, String analyticType, String dataSetType) {
        this.meta = meta;
        this.obsval = obsval;
        this.msgid = msgid;
        this.res = res;
        this.analyticType = analyticType;
        this.dataSetType = dataSetType;
    }

    public DecisionTreeEntry() {
    }

    public String getMeta() {
        return meta;
    }

    public void setMeta(String meta) {
        this.meta = meta;
    }

    public String getObsval() {
        return obsval;
    }

    public void setObsval(String obsval) {
        this.obsval = obsval;
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    public String getRes() {
        return res;
    }

    public void setRes(String res) {
        this.res = res;
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
        DecisionTreeEntry that = (DecisionTreeEntry) o;
        return Objects.equals(msgid, that.msgid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgid);
    }

    @Override
    public String toString() {
        return "DecisionTreeEntry{"
                + "meta='"
                + meta
                + '\''
                + ", obsval='"
                + obsval
                + '\''
                + ", msgid='"
                + msgid
                + '\''
                + ", res='"
                + res
                + '\''
                + ", analyticType='"
                + analyticType
                + '\''
                + '}';
    }
}
