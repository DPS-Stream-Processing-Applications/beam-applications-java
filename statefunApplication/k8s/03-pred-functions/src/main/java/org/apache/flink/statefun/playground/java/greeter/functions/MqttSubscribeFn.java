package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.types.generated.MqttSubscribeEntry;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.MQTT_SUBSCRIBE_ENTRY_PROTOBUF_TYPE;

public class MqttSubscribeFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/mqttSubscribe");

    static final TypeName INBOX = TypeName.typeNameFromString("pred/blobRead");

    private static final ValueSpec<Long> MSGID_COUNT = ValueSpec
            .named("message_counter")
            .withLongType();


    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withValueSpec(MSGID_COUNT)
                    .withSupplier(MqttSubscribeFn::new)
                    .build();


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        long msgId = context.storage().get(MSGID_COUNT).orElse(1L);
        String rowString = new String(message.rawValue().toByteArray(), StandardCharsets.UTF_8);

        MqttSubscribeEntry mqttSubscribeEntry =
                MqttSubscribeEntry.newBuilder()
                        .setAnalaytictype(rowString.split("-")[1])
                        .setBlobModelPath(rowString)
                        .setMsgid(Long.toString(msgId))
                        .setDataSetType("TAXI").build();
        msgId += 1;
        context.storage().set(MSGID_COUNT, msgId);
        context.send(
                MessageBuilder.forAddress(INBOX, String.valueOf(mqttSubscribeEntry.getMsgid()))
                        .withCustomType(MQTT_SUBSCRIBE_ENTRY_PROTOBUF_TYPE, mqttSubscribeEntry)
                        .build());

        return context.done();
    }
}
