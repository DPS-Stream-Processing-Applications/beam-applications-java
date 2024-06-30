package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;

public class MqttPublishEntry implements Serializable {
  private String msgid;
  private long arrivalTime;

  public MqttPublishEntry(String msgid, long arrivalTime) {
    this.msgid = msgid;
    this.arrivalTime = arrivalTime;
  }

  public String getMsgid() {
    return msgid;
  }

  public void setMsgid(String msgid) {
    this.msgid = msgid;
  }

  public long getArrivalTime() {
    return arrivalTime;
  }

  public void setArrivalTime(long arrivalTime) {
    this.arrivalTime = arrivalTime;
  }

  @Override
  public String toString() {
    return "MqttPublishEntry{" + "msgid='" + msgid + '\'' + '}';
  }
}
