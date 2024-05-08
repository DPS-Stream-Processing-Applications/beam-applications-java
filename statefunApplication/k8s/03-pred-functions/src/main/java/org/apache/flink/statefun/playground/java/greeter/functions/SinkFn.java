package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.types.MqttPublishEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.MQTT_PUBLISH_ENTRY_JSON_TYPE;

public class SinkFn implements StatefulFunction {
    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/sink");

    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(SinkFn::new)
                    .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        MqttPublishEntry mqttPublishEntry = message.as(MQTT_PUBLISH_ENTRY_JSON_TYPE);
        System.out.println(mqttPublishEntry);
        return context.done();
    }
}
