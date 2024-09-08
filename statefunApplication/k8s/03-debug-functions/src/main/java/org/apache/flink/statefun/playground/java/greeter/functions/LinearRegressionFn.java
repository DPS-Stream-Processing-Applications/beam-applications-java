package org.apache.flink.statefun.playground.java.greeter.functions;


import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.LinearRegressionPredictor;
import org.apache.flink.statefun.playground.java.greeter.types.BlobReadEntry;
import org.apache.flink.statefun.playground.java.greeter.types.LinearRegressionEntry;
import org.apache.flink.statefun.playground.java.greeter.types.SenMlEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.classifiers.functions.LinearRegression;
import weka.core.SerializationHelper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.*;

public class LinearRegressionFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/linearRegression");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(LinearRegressionFn::new)
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
        try {
            long arrivalTime;
            if (message.is(SENML_ENTRY_JSON_TYPE)) {
                SenMlEntry senMlEntry = message.as(SENML_ENTRY_JSON_TYPE);
                arrivalTime = senMlEntry.getArrivalTime();
                String sensorMeta = senMlEntry.getMeta();
                String msgtype = senMlEntry.getMsgtype();
                String obsVal = senMlEntry.getObsVal();
                String msgId = senMlEntry.getMsgid();
                calculatePrimes(1000);

                Thread.sleep(1000*2);
                Float res = 2.0f;
                if (res != null) {
                    if (res != Float.MIN_VALUE) {
                        LinearRegressionEntry linearRegressionEntry = new LinearRegressionEntry(sensorMeta, obsVal, msgId, res.toString(), "MLR", senMlEntry.getDataSetType());
                        //linearRegressionEntry.setArrivalTime(arrivalTime);
                        context.send(
                                MessageBuilder.forAddress(INBOX, String.valueOf(linearRegressionEntry.getMsgid()))
                                        .withCustomType(LINEAR_REGRESSION_ENTRY_JSON_TYPE, linearRegressionEntry)
                                        .build());

                    } else {
                        LinearRegressionEntry linearRegressionEntry = new LinearRegressionEntry(sensorMeta, obsVal, msgId, "0", "MLR", senMlEntry.getDataSetType());
                        //linearRegressionEntry.setArrivalTime(arrivalTime);

                        context.send(
                                MessageBuilder.forAddress(INBOX, String.valueOf(linearRegressionEntry.getMsgid()))
                                        .withCustomType(LINEAR_REGRESSION_ENTRY_JSON_TYPE, linearRegressionEntry)
                                        .build());



                    }
                }


            } else if (message.is(BLOB_READ_ENTRY_JSON_TYPE)) {

                BlobReadEntry blobReadEntry = message.as(BLOB_READ_ENTRY_JSON_TYPE);
                arrivalTime = blobReadEntry.getArrivalTime();
                String sensorMeta = blobReadEntry.getMeta();
                String msgtype = blobReadEntry.getMsgType();
                String analyticsType = blobReadEntry.getAnalyticType();

                String obsVal = "";
                String msgId = blobReadEntry.getMsgid();

               Float res = 12f;
                if (res != null) {
                    if (res != Float.MIN_VALUE) {
                        LinearRegressionEntry linearRegressionEntry = new LinearRegressionEntry(sensorMeta, obsVal, msgId, res.toString(), "MLR", blobReadEntry.getDataSetType());
                        //linearRegressionEntry.setArrivalTime(arrivalTime);
                        context.send(
                                MessageBuilder.forAddress(INBOX, String.valueOf(linearRegressionEntry.getMsgid()))
                                        .withCustomType(LINEAR_REGRESSION_ENTRY_JSON_TYPE, linearRegressionEntry)
                                        .build());
                    } else {
                        throw new RuntimeException("Res is null or float.min");
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return context.done();
    }
}
