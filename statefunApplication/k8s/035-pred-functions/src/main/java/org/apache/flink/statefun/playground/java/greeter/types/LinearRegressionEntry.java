package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class LinearRegressionEntry {

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

    public LinearRegressionEntry(
            String meta, String obsval, String msgid, String res, String analyticType, String dataSetType) {
        this.meta = meta;
        this.obsval = obsval;
        this.msgid = msgid;
        this.res = res;
        this.analyticType = analyticType;
        this.dataSetType = dataSetType;
    }

    public LinearRegressionEntry() {
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

    public String getDataSetType() {
        return dataSetType;
    }

    public void setDataSetType(String dataSetType) {
        this.dataSetType = dataSetType;
    }

    public String getAnalyticType() {
        return analyticType;
    }

    public void setAnalyticType(String analyticType) {
        this.analyticType = analyticType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LinearRegressionEntry that = (LinearRegressionEntry) o;
        return Objects.equals(msgid, that.msgid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgid);
    }

    @Override
    public String toString() {
        return "LinearRegressionEntry{"
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
