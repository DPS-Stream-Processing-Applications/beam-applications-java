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
    private  Logger l;

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

    public  void initLogger(Logger l_) {
        this.l = l_;
    }

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        initLogger(LoggerFactory.getLogger("APP"));
        calculatePrimes(1000);
        MqttPublishEntry mqttPublishEntry = message.as(MQTT_PUBLISH_ENTRY_JSON_TYPE);
        /*
        if (mqttPublishEntry.getArrivalTime() != 0L) {
            long latency = System.currentTimeMillis() - mqttPublishEntry.getArrivalTime();
            l.warn("Latency: " + latency);
        }
         */
        return context.done();
    }
}