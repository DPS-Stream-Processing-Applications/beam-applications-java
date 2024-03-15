package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;
import java.util.Objects;

public class SourceEntry implements Serializable {
    private String msgid;
    private String payLoad;

    public SourceEntry(String msgid, String payLoad) {
        this.msgid = msgid;
        this.payLoad = payLoad;
    }

    public SourceEntry() {
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    public String getPayLoad() {
        return payLoad;
    }

    public void setPayLoad(String payLoad) {
        this.payLoad = payLoad;
    }

    @Override
    public String toString() {
        return "SourceEntry{" + "msgid='" + msgid + '\'' + ", payLoad='" + payLoad + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SourceEntry that = (SourceEntry) o;
        return Objects.equals(msgid, that.msgid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgid);
    }
}
