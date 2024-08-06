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
    private static Logger l;
    Map<String, BlockWindowAverage> blockWindowAverageMap;
    private String dataSetType;
    private Properties p;
    private ArrayList<String> useMsgList;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    public void setup(String dataSetType) {
        this.dataSetType = dataSetType;
        this.p = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get("/resources/all_tasks.properties"))) {
            this.p.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        blockWindowAverageMap = new HashMap<>();
        String useMsgField = p.getProperty("AGGREGATE.BLOCK_AVERAGE.USE_MSG_FIELD");
        String[] msgField = useMsgField.split(",");
        useMsgList = new ArrayList<>();
        Collections.addAll(useMsgList, msgField);
        initLogger(l);
    }

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        SenMlEntry senMlEntry = message.as(SENML_ENTRY_JSON_TYPE);
        setup(senMlEntry.getDataSetType());
        String msgId = senMlEntry.getMsgid();
        String sensorMeta = senMlEntry.getMeta();
        String sensorID = senMlEntry.getSensorID();
        String obsType = senMlEntry.getObsType();
        String obsVal = senMlEntry.getObsVal();

        HashMap<String, String> map = new HashMap<>();
        if (dataSetType.equals("TAXI")) {
            // obsVal = "12,13,14";
            String fare_amount = obsVal.split(",")[2]; // fare_amount as last obs. in input
            map.put(AbstractTask.DEFAULT_KEY, fare_amount);
        }

        if (dataSetType.equals("SYS")) {
            String airquality = obsVal.split(",")[4]; // airquality as last obs. in input
            map.put(AbstractTask.DEFAULT_KEY, airquality);
        }

        if (dataSetType.equals("FIT")) {
            String fare_amount = obsVal.split(",")[7];
            map.put(AbstractTask.DEFAULT_KEY, fare_amount);
        }

        if (useMsgList.contains(obsType)) {
            String key = sensorID + obsType;
            BlockWindowAverage blockWindowAverage = blockWindowAverageMap.get(key);
            if (blockWindowAverage == null) {
                blockWindowAverage = new BlockWindowAverage();
                blockWindowAverage.setup(l, p);
                blockWindowAverageMap.put(key, blockWindowAverage);
            }

            blockWindowAverage.doTask(map);

            Float avgres =
                    blockWindowAverage.getLastResult(); //  Avg of last window is used till next comes
            sensorMeta = sensorMeta.concat(",").concat(obsType);

            if (dataSetType.equals("TAXI") | dataSetType.equals("FIT")) {
                obsType = "fare_amount";
            }
            if (dataSetType.equals("SYS")) {
                obsType = "AVG";
            }

            if (avgres != null) {
                if (avgres != Float.MIN_VALUE) {
                    if (l.isInfoEnabled()) l.info("avgres AVG:{}", avgres);

                    AverageEntry averageEntry =
                            new AverageEntry(
                                    sensorMeta, sensorID, obsType, avgres.toString(), obsVal, msgId, "AVG", senMlEntry.getDataSetType());
                    //averageEntry.setArrivalTime(senMlEntry.getArrivalTime());
                    context.send(
                            MessageBuilder.forAddress(INBOX, String.valueOf(averageEntry.getMsgid()))
                                    .withCustomType(AVERAGE_ENTRY_JSON_TYPE, averageEntry)
                                    .build());
                } else {
                    if (l.isWarnEnabled()) l.warn("Error in BlockWindowAverageBolt");
                    throw new RuntimeException();
                }
            }
        }
        return context.done();
    }
}
