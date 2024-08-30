package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.types.BlobUploadEntry;
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

import static org.apache.flink.statefun.playground.java.greeter.types.Types.BlobUpload_ENTRY_JSON_TYPE;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.MqttPublish_ENTRY_JSON_TYPE;

public class MqttPublishTrainFn implements StatefulFunction {


    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/mqttPublishTrain");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(MqttPublishTrainFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/sinkTrain");
    static final TypeName KAFKA_EGRESS = TypeName.typeNameFromString("pred/publish");
    private static Logger l;
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public CompletableFuture<Void> apply(Context context, Message message) {
        initLogger(LoggerFactory.getLogger("APP"));
        try {
            BlobUploadEntry blobUploadEntry = message.as(BlobUpload_ENTRY_JSON_TYPE);
            String msgId = blobUploadEntry.getMsgid();
            String filename = blobUploadEntry.getFileName();
            MqttPublishEntry mqttPublishEntry = new MqttPublishEntry(msgId);
            mqttPublishEntry.setArrivalTime(blobUploadEntry.getArrivalTime());
            context.send(
                    MessageBuilder.forAddress(INBOX, String.valueOf(mqttPublishEntry.getMsgid()))
                            .withCustomType(MqttPublish_ENTRY_JSON_TYPE, mqttPublishEntry)
                            .build());
            context.send(
                    KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                            .withTopic("pred-publish")
                            .withUtf8Key(String.valueOf(System.currentTimeMillis()))
                            .withUtf8Value(String.valueOf(filename))
                            .build());


        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error in ParsePredictBeam " + e);
        }
        return context.done();
    }
}

