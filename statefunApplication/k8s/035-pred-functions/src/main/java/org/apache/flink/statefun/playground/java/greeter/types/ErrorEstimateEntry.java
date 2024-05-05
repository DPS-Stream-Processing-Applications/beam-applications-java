package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class ErrorEstimateEntry {

    @JsonProperty("meta")
    private String meta;

    @JsonProperty("error")
    private float error;

    @JsonProperty("msgid")
    private String msgid;

    @JsonProperty("analyticType")
    private String analyticType;
    @JsonProperty("obsval")
    private String obsval;

    @JsonProperty("datasetType")
    private String dataSetType;

    public ErrorEstimateEntry(
            String meta, float error, String msgid, String analyticType, String obsval, String dataSetType) {
        this.meta = meta;
        this.error = error;
        this.msgid = msgid;
        this.analyticType = analyticType;
        this.obsval = obsval;
        this.dataSetType = dataSetType;
    }

    public ErrorEstimateEntry() {
    }

    public String getMeta() {
        return meta;
    }

    public void setMeta(String meta) {
        this.meta = meta;
    }

    public float getError() {
        return error;
    }

    public void setError(float error) {
        this.error = error;
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

    public String getObsval() {
        return obsval;
    }

    public void setObsval(String obsval) {
        this.obsval = obsval;
    }

    public String getDataSetType() {
        return dataSetType;
    }

    public void setDataSetType(String dataSetType) {
        this.dataSetType = dataSetType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorEstimateEntry that = (ErrorEstimateEntry) o;
        return Objects.equals(msgid, that.msgid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgid);
    }

    @Override
    public String toString() {
        return "ErrorEstimateEntry{"
                + "meta='"
                + meta
                + '\''
                + ", error='"
                + error
                + '\''
                + ", msgid='"
                + msgid
                + '\''
                + ", analyticType='"
                + analyticType
                + '\''
                + ", obsval='"
                + obsval
                + '\''
                + '}';
    }


}
