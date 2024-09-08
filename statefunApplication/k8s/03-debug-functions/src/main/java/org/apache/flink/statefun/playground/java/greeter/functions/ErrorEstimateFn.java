package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.types.AverageEntry;
import org.apache.flink.statefun.playground.java.greeter.types.ErrorEstimateEntry;
import org.apache.flink.statefun.playground.java.greeter.types.LinearRegressionEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.*;

public class ErrorEstimateFn implements StatefulFunction {
    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/errorEstimate");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(ErrorEstimateFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/mqttPublish");

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
            long arrivalTime;
            if (message.is(LINEAR_REGRESSION_ENTRY_JSON_TYPE)) {
                LinearRegressionEntry linearRegressionEntry = message.as(LINEAR_REGRESSION_ENTRY_JSON_TYPE);
                String msgId = linearRegressionEntry.getMsgid();
                String analyticsType = linearRegressionEntry.getAnalyticType();

                String sensorMeta = linearRegressionEntry.getMeta();
                String obsVal = linearRegressionEntry.getObsval();
                calculatePrimes(1000);
                float errval = 4f;
                    ErrorEstimateEntry errorEstimateEntry = new ErrorEstimateEntry(sensorMeta, errval, msgId, analyticsType, obsVal, linearRegressionEntry.getDataSetType());
                    //errorEstimateEntry.setArrivalTime(linearRegressionEntry.getArrivalTime());
                    context.send(
                            MessageBuilder.forAddress(INBOX, String.valueOf(errorEstimateEntry.getMsgid()))
                                    .withCustomType(ERROR_ESTIMATE_ENTRY_JSON_TYPE, errorEstimateEntry)
                                    .build());


            } else if (message.is(AVERAGE_ENTRY_JSON_TYPE)) {
                AverageEntry averageEntry = message.as(AVERAGE_ENTRY_JSON_TYPE);
                arrivalTime = averageEntry.getArrivalTime();
                String msgId = averageEntry.getMsgid();
                String analyticsType = averageEntry.getAnalyticType();
                String sensorMeta = averageEntry.getMeta();
                String obsVal = averageEntry.getObsVal();


                if (analyticsType.equals("MLR")) {
                    calculatePrimes(1000);
                    float errval = 0;
                    ErrorEstimateEntry errorEstimateEntry = new ErrorEstimateEntry(sensorMeta, errval, msgId, analyticsType, obsVal, averageEntry.getDataSetType());
                    //errorEstimateEntry.setArrivalTime(arrivalTime);
                    context.send(
                            MessageBuilder.forAddress(INBOX, String.valueOf(errorEstimateEntry.getMsgid()))
                                    .withCustomType(ERROR_ESTIMATE_ENTRY_JSON_TYPE, errorEstimateEntry)
                                    .build());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return context.done();
    }
}
