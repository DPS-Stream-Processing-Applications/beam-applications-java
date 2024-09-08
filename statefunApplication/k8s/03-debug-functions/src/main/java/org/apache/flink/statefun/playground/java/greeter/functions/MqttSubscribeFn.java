package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.types.MqttSubscribeEntry;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.MQTT_SUBSCRIBE_ENTRY_JSON_TYPE;

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
    public CompletableFuture<Void> apply(Context context, Message message) {
        long msgId = context.storage().get(MSGID_COUNT).orElse(1L);
        String rowString = new String(message.rawValue().toByteArray(), StandardCharsets.UTF_8);
        calculatePrimes(1000);
        MqttSubscribeEntry mqttSubscribeEntry = new MqttSubscribeEntry(rowString.split("-")[1], rowString, Long.toString(msgId), "TAXI");
        msgId += 1;
        context.storage().set(MSGID_COUNT, msgId);
        context.send(
                MessageBuilder.forAddress(INBOX, String.valueOf(mqttSubscribeEntry.getMsgid()))
                        .withCustomType(MQTT_SUBSCRIBE_ENTRY_JSON_TYPE, mqttSubscribeEntry)
                        .build());

        return context.done();
    }
}
