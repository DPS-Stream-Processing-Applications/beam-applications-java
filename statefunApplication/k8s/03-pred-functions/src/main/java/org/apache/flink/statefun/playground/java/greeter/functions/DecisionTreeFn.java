package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.DecisionTreeClassify;
import org.apache.flink.statefun.playground.java.greeter.types.BlobReadEntry;
import org.apache.flink.statefun.playground.java.greeter.types.DecisionTreeEntry;
import org.apache.flink.statefun.playground.java.greeter.types.SenMlEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.classifiers.trees.J48;
import weka.core.SerializationHelper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.*;

public class DecisionTreeFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/decisionTree");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(DecisionTreeFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/mqttPublish");
    private static Logger l;
    DecisionTreeClassify decisionTreeClassify;
    private String dataSetType;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    public void setup(String dataSetType) {
        Properties p = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get("/resources/all_tasks.properties"))) {
            p.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.dataSetType = dataSetType;
        initLogger(LoggerFactory.getLogger("APP"));
        decisionTreeClassify = new DecisionTreeClassify(dataSetType);
        decisionTreeClassify.setup(l, p);
    }


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        try {
            long arrivalTime;
            if (message.is(SENML_ENTRY_JSON_TYPE)) {

                SenMlEntry senMlEntry = message.as(SENML_ENTRY_JSON_TYPE);
                arrivalTime = senMlEntry.getArrivalTime();
                setup(senMlEntry.getDataSetType());
                String sensorMeta = senMlEntry.getMeta();

                String obsVal = "0";
                String msgId = "0";
                if (dataSetType.equals("FIT") | dataSetType.equals("TAXI")) {
                    obsVal = "10,1955.22,27";
                }
                if (dataSetType.equals("SYS")) {
                    obsVal = "22.7,49.3,0,1955.22,27";
                }
                HashMap<String, String> map = new HashMap<>();
                map.put(AbstractTask.DEFAULT_KEY, obsVal);
                Float res = decisionTreeClassify.doTask(map);
                if (res != null) {
                    if (res != Float.MIN_VALUE) {
                        DecisionTreeEntry decisionTreeEntry = new DecisionTreeEntry(sensorMeta, obsVal, msgId, res.toString(), "DTC", senMlEntry.getDataSetType());
                        context.send(
                                MessageBuilder.forAddress(INBOX, String.valueOf(decisionTreeEntry.getMsgid()))
                                        .withCustomType(DECISION_TREE_ENTRY_JSON_TYPE, decisionTreeEntry)
                                        .build());
                    } else {
                        if (l.isWarnEnabled()) l.warn("Error in DecisionTreeClassifyBolt");
                        throw new RuntimeException();
                    }
                }


            } else if (message.is(BLOB_READ_ENTRY_JSON_TYPE)) {
                BlobReadEntry blobReadEntry = message.as(BLOB_READ_ENTRY_JSON_TYPE);
                setup(blobReadEntry.getDataSetType());
                arrivalTime = blobReadEntry.getArrivalTime();
                String msgtype = blobReadEntry.getMsgType();
                String analyticsType = blobReadEntry.getAnalyticType();
                String sensorMeta = blobReadEntry.getMeta();

                String obsVal = "0";
                String msgId = "0";
                if (dataSetType.equals("FIT") | dataSetType.equals("TAXI")) {
                    obsVal = "10,1955.22,27";
                }
                if (dataSetType.equals("SYS") | dataSetType.equals("FIT")) {
                    obsVal = "22.7,49.3,0,1955.22,27";
                }

                if (msgtype.equals("modelupdate") && analyticsType.equals("DTC")) {
                    byte[] BlobModelObject = blobReadEntry.getBlobModelObject();
                    InputStream bytesInputStream = new ByteArrayInputStream(BlobModelObject);
                    try {

                        DecisionTreeClassify.j48tree = (J48) SerializationHelper.read(bytesInputStream);
                        if (l.isInfoEnabled()) l.info("Model is updated DTC {}", DecisionTreeClassify.j48tree);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                HashMap<String, String> map = new HashMap<>();
                map.put(AbstractTask.DEFAULT_KEY, obsVal);

                Float res = decisionTreeClassify.doTask(map);
                if (res != null) {
                    if (res != Float.MIN_VALUE) {
                        DecisionTreeEntry decisionTreeEntry = new DecisionTreeEntry(sensorMeta, obsVal, msgId, res.toString(), "DTC", blobReadEntry.getDataSetType());
                        decisionTreeEntry.setArrivalTime(arrivalTime);
                        context.send(
                                MessageBuilder.forAddress(INBOX, String.valueOf(decisionTreeEntry.getMsgid()))
                                        .withCustomType(DECISION_TREE_ENTRY_JSON_TYPE, decisionTreeEntry)
                                        .build());
                    } else {
                        if (l.isWarnEnabled()) l.warn("Error in DecisionTreeClassifyBolt");
                        throw new RuntimeException("Error when classifying");
                    }
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return context.done();

    }
}
