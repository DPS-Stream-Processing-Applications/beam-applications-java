package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class MqttPublishEntry {

    @JsonProperty("msgid")
    private String msgid;

    @JsonProperty("meta")
    private String meta;

    @JsonProperty("obsval")
    private String obsval;

    @JsonProperty("datasetType")
    private String dataSetType;

    @JsonProperty("arrivalTime")
    private long arrivalTime;

    public MqttPublishEntry(String msgid, String meta, String obsval, String dataSetType) {
        this.msgid = msgid;
        this.meta = meta;
        this.obsval = obsval;
        this.dataSetType = dataSetType;
    }

    public MqttPublishEntry() {
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
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
        MqttPublishEntry that = (MqttPublishEntry) o;
        return Objects.equals(msgid, that.msgid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgid);
    }

    @Override
    public String toString() {
        return "MqttPublishEntry{"
                + "msgid='"
                + msgid
                + '\''
                + ", meta='"
                + meta
                + '\''
                + ", obsval='"
                + obsval
                + '\''
                + '}';
    }

}
