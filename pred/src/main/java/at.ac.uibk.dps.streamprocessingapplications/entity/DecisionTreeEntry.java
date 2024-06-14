package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;
import java.util.Objects;

public class DecisionTreeEntry implements Serializable, MqttPublishInput {
  private String meta;
  private String obsval;
  private String msgid;
  private String res;
  private String analyticType;

  public DecisionTreeEntry(
      String meta, String obsval, String msgid, String res, String analyticType) {
    this.meta = meta;
    this.obsval = obsval;
    this.msgid = msgid;
    this.res = res;
    this.analyticType = analyticType;
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
