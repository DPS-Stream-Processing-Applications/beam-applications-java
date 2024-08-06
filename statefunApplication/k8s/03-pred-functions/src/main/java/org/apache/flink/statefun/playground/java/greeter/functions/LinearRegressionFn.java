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
    private static Logger l;
    LinearRegressionPredictor linearRegressionPredictor = null;
    private String dataSetType;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    public void setup(String datasetType) {
        Properties p = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get("/resources/all_tasks.properties"))) {
            p.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.dataSetType = datasetType;
        if (linearRegressionPredictor == null) {
            String ipAddress = System.getenv("MONGO_DB_ADDRESS");
            linearRegressionPredictor = new LinearRegressionPredictor(ipAddress, "mydb");
            initLogger(LoggerFactory.getLogger("APP"));
            linearRegressionPredictor.setup(l, p);
        }
    }


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        initLogger(LoggerFactory.getLogger("APP"));
        try {
            long arrivalTime;
            if (message.is(SENML_ENTRY_JSON_TYPE)) {
                SenMlEntry senMlEntry = message.as(SENML_ENTRY_JSON_TYPE);
                setup(senMlEntry.getDataSetType());
                arrivalTime = senMlEntry.getArrivalTime();
                this.dataSetType = senMlEntry.getDataSetType();
                String sensorMeta = senMlEntry.getMeta();
                String msgtype = senMlEntry.getMsgtype();
                String obsVal = senMlEntry.getObsVal();
                String msgId = senMlEntry.getMsgid();

                if (!msgtype.equals("modelupdate")) {
                    obsVal = senMlEntry.getObsVal();
                    msgId = senMlEntry.getMsgid();

                    if (l.isInfoEnabled()) l.info("modelupdate obsVal-" + obsVal);
                }

                HashMap<String, String> map = new HashMap<>();
                map.put(AbstractTask.DEFAULT_KEY, obsVal);
                Float res = linearRegressionPredictor.doTask(map);
                //Float res = 2.0f;

                if (l.isInfoEnabled()) l.info("res linearRegressionPredictor-" + res);

                if (res != null) {
                    if (res != Float.MIN_VALUE) {
                        LinearRegressionEntry linearRegressionEntry = new LinearRegressionEntry(sensorMeta, obsVal, msgId, res.toString(), "MLR", senMlEntry.getDataSetType());
                        //linearRegressionEntry.setArrivalTime(arrivalTime);
                        context.send(
                                MessageBuilder.forAddress(INBOX, String.valueOf(linearRegressionEntry.getMsgid()))
                                        .withCustomType(LINEAR_REGRESSION_ENTRY_JSON_TYPE, linearRegressionEntry)
                                        .build());
                    } else {
                        if (l.isWarnEnabled()) l.warn("Error in LinearRegressionPredictorBolt");
                        throw new RuntimeException("Res is null or float.min");
                    }
                }


            } else if (message.is(BLOB_READ_ENTRY_JSON_TYPE)) {

                BlobReadEntry blobReadEntry = message.as(BLOB_READ_ENTRY_JSON_TYPE);
                setup(blobReadEntry.getDataSetType());
                arrivalTime = blobReadEntry.getArrivalTime();
                this.dataSetType = blobReadEntry.getDataSetType();
                String sensorMeta = blobReadEntry.getMeta();
                String msgtype = blobReadEntry.getMsgType();
                String analyticsType = blobReadEntry.getAnalyticType();

                String obsVal = "";
                String msgId = blobReadEntry.getMsgid();

                if (dataSetType.equals("TAXI") | dataSetType.equals("FIT")) {
                    obsVal = "10,1955.22,27";
                }
                if (dataSetType.equals("SYS")) {
                    obsVal = "22.7,49.3,0,1955.22,27";
                }

                if (msgtype.equals("modelupdate") && analyticsType.equals("MLR")) {
                    byte[] BlobModelObject = blobReadEntry.getBlobModelObject();
                    InputStream bytesInputStream = new ByteArrayInputStream(BlobModelObject);

                    try {
                        LinearRegressionPredictor.lr =
                                (LinearRegression) SerializationHelper.read(bytesInputStream);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException("Error, when reading BlobObject " + e);
                    }
                    if (this.l.isInfoEnabled()) {
                        this.l.info("Model is updated MLR {} ", LinearRegressionPredictor.lr.toString());
                    }
                }

                if (!msgtype.equals("modelupdate")) {
                    msgId = blobReadEntry.getMsgid();
                }

                HashMap<String, String> map = new HashMap<>();
                map.put(AbstractTask.DEFAULT_KEY, obsVal);

                Float res = linearRegressionPredictor.doTask(map);

                if (l.isInfoEnabled()) l.info("res linearRegressionPredictor-" + res);

                if (res != null) {
                    if (res != Float.MIN_VALUE) {
                        LinearRegressionEntry linearRegressionEntry = new LinearRegressionEntry(sensorMeta, obsVal, msgId, res.toString(), "MLR", blobReadEntry.getDataSetType());
                        //linearRegressionEntry.setArrivalTime(arrivalTime);
                        context.send(
                                MessageBuilder.forAddress(INBOX, String.valueOf(linearRegressionEntry.getMsgid()))
                                        .withCustomType(LINEAR_REGRESSION_ENTRY_JSON_TYPE, linearRegressionEntry)
                                        .build());
                    } else {
                        if (l.isWarnEnabled()) l.warn("Error in LinearRegressionPredictorBolt");
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
