package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.BlockWindowAverage;
import org.apache.flink.statefun.playground.java.greeter.types.AverageEntry;
import org.apache.flink.statefun.playground.java.greeter.types.SenMlEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.AVERAGE_ENTRY_JSON_TYPE;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.SENML_ENTRY_JSON_TYPE;


public class AverageFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/average");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(AverageFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/errorEstimate");

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
        SenMlEntry senMlEntry = message.as(SENML_ENTRY_JSON_TYPE);
        String msgId = senMlEntry.getMsgid();
        String sensorMeta = senMlEntry.getMeta();
        String sensorID = senMlEntry.getSensorID();
        String obsType = senMlEntry.getObsType();
        String obsVal = senMlEntry.getObsVal();

        Float avgres=2f;
        calculatePrimes(1000);

            if (avgres != null) {
                if (avgres != Float.MIN_VALUE) {
                    AverageEntry averageEntry =
                            new AverageEntry(
                                    sensorMeta, sensorID, obsType, avgres.toString(), obsVal, msgId, "AVG", senMlEntry.getDataSetType());
                    //averageEntry.setArrivalTime(senMlEntry.getArrivalTime());
                    context.send(
                            MessageBuilder.forAddress(INBOX, String.valueOf(averageEntry.getMsgid()))
                                    .withCustomType(AVERAGE_ENTRY_JSON_TYPE, averageEntry)
                                    .build());
                } else {
                    throw new RuntimeException();
                }
            }
        return context.done();
    }
}
