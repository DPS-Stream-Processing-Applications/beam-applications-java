package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.types.generated.DecisionTreeEntry;
import org.apache.flink.statefun.playground.java.greeter.types.generated.ErrorEstimateEntry;
import org.apache.flink.statefun.playground.java.greeter.types.generated.MqttPublishEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.*;

public class MqttPublishFn implements StatefulFunction {
    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/mqttPublish");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(MqttPublishFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/sink");

    static final TypeName KAFKA_EGRESS = TypeName.typeNameFromString("pred/publish");
    private  Logger l;


    public void initLogger(Logger l_) {
        this.l = l_;
    }

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        initLogger(LoggerFactory.getLogger("APP"));
        try {
            if (message.is(DECISION_TREE_ENTRY_PROTOBUF_TYPE)) {
                DecisionTreeEntry decisionTreeEntry = message.as(DECISION_TREE_ENTRY_PROTOBUF_TYPE);
                String msgId = decisionTreeEntry.getMsgid();
                String meta = decisionTreeEntry.getMeta();
                String analyticsType = decisionTreeEntry.getAnalyticType();
                String obsVal = decisionTreeEntry.getObsval();

                StringBuffer temp = new StringBuffer();
                String res;
                if (analyticsType.equals("DTC")) {
                    res = decisionTreeEntry.getRes();
                    temp.append(msgId)
                            .append(",")
                            .append(meta)
                            .append(",")
                            .append(analyticsType)
                            .append(",obsVal:")
                            .append(obsVal)
                            .append(",RES:")
                            .append(res);
                }

                if (l.isInfoEnabled()) l.info("MQTT result:{}", temp);
                final MqttPublishEntry mqttPublishEntry =
                        MqttPublishEntry.newBuilder()
                                .setMsgid(msgId)
                                        .setMeta(meta)
                                                .setObsval(obsVal)
                                                        .setDataSetType(decisionTreeEntry.getDataSetType()).build();

                //mqttPublishEntry.setArrivalTime(decisionTreeEntry.getArrivalTime());
                context.send(
                        MessageBuilder.forAddress(INBOX, String.valueOf(mqttPublishEntry.getMsgid()))
                                .withCustomType(MQTT_PUBLISH_ENTRY_PROTOBUF_TYPE, mqttPublishEntry)
                                .build());
                context.send(
                        KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                                .withTopic("pred-publish")
                                .withUtf8Key(String.valueOf(System.currentTimeMillis()))
                                .withUtf8Value(String.valueOf(temp))
                                .build());


            } else if (message.is(ERROR_ESTIMATE_ENTRY_PROTOBUF_TYPE)) {
                ErrorEstimateEntry errorEstimateEntry = message.as(ERROR_ESTIMATE_ENTRY_PROTOBUF_TYPE);
                String msgId = errorEstimateEntry.getMsgid();
                String meta = errorEstimateEntry.getMeta();
                String analyticsType = errorEstimateEntry.getAnalyticType();
                String obsVal = errorEstimateEntry.getObsval();

                StringBuffer temp = new StringBuffer();
                String res = "";
                if (analyticsType.equals("MLR")) {
                    res = String.valueOf(errorEstimateEntry.getError());
                    temp.append(msgId)
                            .append(",")
                            .append(meta)
                            .append(",")
                            .append(analyticsType)
                            .append(",obsVal:")
                            .append(obsVal)
                            .append(",ERROR:")
                            .append(res);
                }

                if (l.isInfoEnabled()) l.info("MQTT result:{}", temp);
                final MqttPublishEntry mqttPublishEntry =
                        MqttPublishEntry.newBuilder()
                                .setMsgid(msgId)
                                .setMeta(meta)
                                .setObsval(obsVal)
                                .setArrivalTime(errorEstimateEntry.getArrivalTime())
                                .setDataSetType(errorEstimateEntry.getDataSetType()).build();
                //mqttPublishEntry.setArrivalTime(errorEstimateEntry.getArrivalTime());
                context.send(
                        MessageBuilder.forAddress(INBOX, String.valueOf(mqttPublishEntry.getMsgid()))
                                .withCustomType(MQTT_PUBLISH_ENTRY_PROTOBUF_TYPE, mqttPublishEntry)
                                .build());
                context.send(
                        KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                                .withTopic("pred-publish")
                                .withUtf8Key(String.valueOf(System.currentTimeMillis()))
                                .withUtf8Value(String.valueOf(temp))
                                .build());

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error in ParsePredictBeam " + e);
        }

        return context.done();
    }
}
