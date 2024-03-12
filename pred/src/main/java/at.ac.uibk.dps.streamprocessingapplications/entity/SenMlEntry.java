package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;
import java.util.Objects;

public class SenMlEntry implements Serializable {
    private String msgid;
    private String sensorID;

    private String meta;
    private String obsType;

    private String obsVal;
    private String msgtype;

    private String analyticType;

    public String getSensorID() {
        return sensorID;
    }

    public void setSensorID(String sensorID) {
        this.sensorID = sensorID;
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

    public SenMlEntry(
            String msgid,
            String sensorID,
            String meta,
            String obsType,
            String obsVal,
            String msgtype,
            String analyticType) {
        this.msgid = msgid;
        this.sensorID = sensorID;
        this.meta = meta;
        this.obsType = obsType;
        this.obsVal = obsVal;
        this.msgtype = msgtype;
        this.analyticType = analyticType;
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
