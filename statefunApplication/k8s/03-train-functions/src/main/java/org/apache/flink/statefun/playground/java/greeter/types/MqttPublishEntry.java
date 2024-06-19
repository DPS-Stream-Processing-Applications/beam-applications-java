package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class MqttPublishEntry implements Serializable {

    @JsonProperty("msgid")
    private String msgid;

    @JsonProperty("dataSetType")
    private String dataSetType;

    public MqttPublishEntry(String msgid, String dataSetType) {
        this.msgid = msgid;
        this.dataSetType = dataSetType;
    }

    public MqttPublishEntry() {
    }

    public MqttPublishEntry(String msgid) {
        this.msgid = msgid;
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

    @Override
    public String toString() {
        return "MqttPublishEntry{" + "msgid='" + msgid + '\'' + '}';
    }
}
