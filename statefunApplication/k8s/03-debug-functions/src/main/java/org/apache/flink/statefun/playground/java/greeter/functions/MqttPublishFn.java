package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.types.DecisionTreeEntry;
import org.apache.flink.statefun.playground.java.greeter.types.ErrorEstimateEntry;
import org.apache.flink.statefun.playground.java.greeter.types.MqttPublishEntry;
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

    public  int calculatePrimes(int limit) {
        int count = 0;
        for (int i = 2; i <= limit; i++) {
            if (isPrime(i)) {
                count++;
            }
        }
        return count;
    }

    public  boolean isPrime(int number) {
        if (number <= 1) {
            return false;
        }
        for (int i = 2; i * i <= number; i++) {
            if (number % i == 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        try {
            if (message.is(DECISION_TREE_ENTRY_JSON_TYPE)) {
                DecisionTreeEntry decisionTreeEntry = message.as(DECISION_TREE_ENTRY_JSON_TYPE);
                String msgId = decisionTreeEntry.getMsgid();
                String meta = decisionTreeEntry.getMeta();
                String analyticsType = decisionTreeEntry.getAnalyticType();
                calculatePrimes(1000);
                String obsVal = decisionTreeEntry.getObsval();

                MqttPublishEntry mqttPublishEntry = new MqttPublishEntry(msgId, meta, obsVal, decisionTreeEntry.getDataSetType());
                //mqttPublishEntry.setArrivalTime(decisionTreeEntry.getArrivalTime());
                context.send(
                        MessageBuilder.forAddress(INBOX, String.valueOf(mqttPublishEntry.getMsgid()))
                                .withCustomType(MQTT_PUBLISH_ENTRY_JSON_TYPE, mqttPublishEntry)
                                .build());
                context.send(
                        KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                                .withTopic("pred-publish")
                                .withUtf8Key(String.valueOf(System.currentTimeMillis()))
                                .withUtf8Value(String.valueOf("temp"))
                                .build());


            } else if (message.is(ERROR_ESTIMATE_ENTRY_JSON_TYPE)) {
                ErrorEstimateEntry errorEstimateEntry = message.as(ERROR_ESTIMATE_ENTRY_JSON_TYPE);
                String msgId = errorEstimateEntry.getMsgid();
                String meta = errorEstimateEntry.getMeta();
                String analyticsType = errorEstimateEntry.getAnalyticType();
                String obsVal = errorEstimateEntry.getObsval();

                MqttPublishEntry mqttPublishEntry = new MqttPublishEntry(msgId, meta, obsVal, errorEstimateEntry.getDataSetType());
                //mqttPublishEntry.setArrivalTime(errorEstimateEntry.getArrivalTime());
                context.send(
                        MessageBuilder.forAddress(INBOX, String.valueOf(mqttPublishEntry.getMsgid()))
                                .withCustomType(MQTT_PUBLISH_ENTRY_JSON_TYPE, mqttPublishEntry)
                                .build());
                context.send(
                        KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                                .withTopic("pred-publish")
                                .withUtf8Key(String.valueOf(System.currentTimeMillis()))
                                .withUtf8Value(String.valueOf("temp"))
                                .build());

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error in ParsePredictBeam " + e);
        }

        return context.done();
    }
}
