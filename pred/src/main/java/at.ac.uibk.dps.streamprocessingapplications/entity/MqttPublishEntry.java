package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;
import java.util.Objects;

public class MqttPublishEntry implements Serializable {
    private String msgid;
    private String meta;
    private String obsval;

    public MqttPublishEntry(String msgid, String meta, String obsval) {
        this.msgid = msgid;
        this.meta = meta;
        this.obsval = obsval;
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
