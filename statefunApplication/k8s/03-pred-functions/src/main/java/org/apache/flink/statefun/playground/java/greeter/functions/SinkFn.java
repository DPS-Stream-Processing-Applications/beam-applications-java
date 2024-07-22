package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.types.MqttPublishEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.MQTT_PUBLISH_ENTRY_JSON_TYPE;

public class SinkFn implements StatefulFunction {
    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/sink");

    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(SinkFn::new)
                    .build();
    private static Logger l;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        initLogger(LoggerFactory.getLogger("APP"));
        MqttPublishEntry mqttPublishEntry = message.as(MQTT_PUBLISH_ENTRY_JSON_TYPE);
        if (mqttPublishEntry.getArrivalTime() != 0L) {
            long latency = System.currentTimeMillis() - mqttPublishEntry.getArrivalTime();
            l.warn("Latency: " + latency);
        }
        return context.done();
    }
}
