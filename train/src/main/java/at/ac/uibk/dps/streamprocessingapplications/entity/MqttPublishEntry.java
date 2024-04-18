package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;

public class MqttPublishEntry implements Serializable {
    private String msgid;

    public MqttPublishEntry(String msgid) {
        this.msgid = msgid;
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    @Override
    public String toString() {
        return "MqttPublishEntry{" + "msgid='" + msgid + '\'' + '}';
    }
}
