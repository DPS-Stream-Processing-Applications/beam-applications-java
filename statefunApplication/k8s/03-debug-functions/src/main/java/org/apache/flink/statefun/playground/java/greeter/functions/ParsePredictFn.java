package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.SenMlParse;
import org.apache.flink.statefun.playground.java.greeter.types.SenMlEntry;
import org.apache.flink.statefun.playground.java.greeter.types.SourceEntry;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.SENML_ENTRY_JSON_TYPE;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.SOURCE_ENTRY_JSON_TYPE;

public class ParsePredictFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/senmlParse");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(ParsePredictFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/decisionTree");
    static final TypeName INBOX_2 = TypeName.typeNameFromString("pred/linearRegression");
    static final TypeName INBOX_3 = TypeName.typeNameFromString("pred/average");

    private boolean doneSetup=false;





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
    private void setup(){
        doneSetup=true;
    }


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        setup();
        try {
            SourceEntry sourceEntry = message.as(SOURCE_ENTRY_JSON_TYPE);
            String msg = sourceEntry.getPayload();
            String msgId = String.valueOf(sourceEntry.getMsgid());

            HashMap<String, String> map = new HashMap<>();
            calculatePrimes(1000);
            map.put(AbstractTask.DEFAULT_KEY, msg);

            HashMap<String, String> resultMap = new HashMap<>();
            resultMap.put("test","test");

            StringBuilder meta = new StringBuilder();
            StringBuilder obsVal = new StringBuilder();
            for (int i = 0; i < 6; i++) {
                meta.append(resultMap.get("test")).append(",");
            }
            meta = meta.deleteCharAt(meta.lastIndexOf(","));
            for (int j = 0; j < 5; j++) {
                obsVal.append(resultMap.get("test"));
                obsVal.append(",");
            }
            obsVal = obsVal.deleteCharAt(obsVal.lastIndexOf(","));

            SenMlEntry senMlEntry =
                    new SenMlEntry(
                            msgId,
                            "1",
                            meta.toString(),
                            "dummyobsType",
                            obsVal.toString(),
                            "MSGTYPE",
                            "DumbType", sourceEntry.getDataSetType());
            //senMlEntry.setArrivalTime(sourceEntry.getArrivalTime());
            context.send(
                    MessageBuilder.forAddress(INBOX_2, String.valueOf(senMlEntry.getMsgid()))
                            .withCustomType(SENML_ENTRY_JSON_TYPE, senMlEntry)
                            .build());


            context.send(
                    MessageBuilder.forAddress(INBOX, String.valueOf(senMlEntry.getMsgid()))
                            .withCustomType(SENML_ENTRY_JSON_TYPE, senMlEntry)
                            .build());




            context.send(
                    MessageBuilder.forAddress(INBOX_3, String.valueOf(senMlEntry.getMsgid()))
                            .withCustomType(SENML_ENTRY_JSON_TYPE, senMlEntry)
                            .build());





        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error in ParsePredictBeam " + e);
        }
        return context.done();
    }
}
