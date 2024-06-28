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

import static org.apache.flink.statefun.playground.java.greeter.types.Types.MqttPublish_ENTRY_JSON_TYPE;

public class SinkTrainFn implements StatefulFunction {
    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/sinkTrain");

    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(SinkTrainFn::new)
                    .build();

    public static void initLogger(Logger l_) {
        l = l_;
    }

    private static Logger l;

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        initLogger(LoggerFactory.getLogger("APP"));
        MqttPublishEntry mqttPublishEntry = message.as(MqttPublish_ENTRY_JSON_TYPE);
        long latency = System.currentTimeMillis()-mqttPublishEntry.getArrivalTime();
        l.info("Latency: "+ latency);
        return context.done();
    }
}
