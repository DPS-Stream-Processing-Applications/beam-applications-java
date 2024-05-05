package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.DecisionTreeTrainBatched;
import org.apache.flink.statefun.playground.java.greeter.types.AnnotateEntry;
import org.apache.flink.statefun.playground.java.greeter.types.TrainEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.ANNOTATE_ENTRY_JSON_TYPE;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.Train_ENTRY_JSON_TYPE;

public class DecisionTreeTrainFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/decisionTreeTrain");

    static final TypeName INBOX = TypeName.typeNameFromString("pred/blobWrite");

    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(DecisionTreeTrainFn::new)
                    .build();

    private static Logger l;
    private String dataSetType;
    DecisionTreeTrainBatched decisionTreeTrainBatched;
    String datasetName;
    private Properties p;

    private String connectionUrl;

    private String dataBaseName;


    public static void initLogger(Logger l_) {
        l = l_;
    }


    public void setup(String dataSetType) throws IOException {
        initLogger(LoggerFactory.getLogger("APP"));
        p = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get("/resources/all_tasks.properties"))) {
            p.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.dataSetType = dataSetType;
        this.connectionUrl = "mongodb://adminuser:password123@192.168.58.2:32000/";
        this.dataBaseName = "mydb";
        decisionTreeTrainBatched = new DecisionTreeTrainBatched(connectionUrl, dataBaseName);
        decisionTreeTrainBatched.setup(l, p);
        if (dataSetType.equals("SYS")) {
            datasetName = p.getProperty("TRAIN.DATASET_NAME_SYS");
        }
        if (dataSetType.equals("TAXI")) {
            datasetName = p.getProperty("TRAIN.DATASET_NAME_TAXI");
        }
        if (dataSetType.equals("FIT")) {
            datasetName = p.getProperty("TRAIN.DATASET_NAME_FIT");
        }
    }


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        AnnotateEntry annotateEntry = message.as(ANNOTATE_ENTRY_JSON_TYPE);
        setup(annotateEntry.getDataSetType());
        String msgid = annotateEntry.getMsgid();
        String annotData = annotateEntry.getAnnotData();
        String rowKeyEnd = annotateEntry.getRowKeyEnd();

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, annotData);

        String filename = "";
        if (dataSetType.equals("SYS")) {
            filename = datasetName + "-DTC-1422748810000.model";
        }

        if (dataSetType.equals("TAXI")) {
            filename = datasetName + "-DTC-1358102664000.model";
        }
        if (dataSetType.equals("FIT")) {
            filename = datasetName + "-DTC-1417890600200.model";
        }

        map.put("FILENAME", filename);
        Float res = decisionTreeTrainBatched.doTask(map);

        if (res != null) {
            if (res != Float.MIN_VALUE) {
                TrainEntry trainEntry = new TrainEntry("model", msgid, rowKeyEnd, "MLR", filename, annotateEntry.getDataSetType());
                context.send(
                        MessageBuilder.forAddress(INBOX, String.valueOf(trainEntry.getMsgid()))
                                .withCustomType(Train_ENTRY_JSON_TYPE, trainEntry)
                                .build());

            } else {
                throw new RuntimeException("Error in DecisionTreeClassifyBeam " + res);
            }
        }


        return context.done();
    }


}
