package at.ac.uibk.dps.streamprocessingapplications.entity;

public interface MqttPublishInput {
  String getMsgid();

  String getMeta();

  String getAnalyticType();

  String getObsval();

  String getRes();
}
