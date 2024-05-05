package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.LinearRegressionTrainBatched;
import org.apache.flink.statefun.playground.java.greeter.types.DbEntry;
import org.apache.flink.statefun.playground.java.greeter.types.TrainEntry;
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
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.Db_ENTRY_JSON_TYPE;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.Train_ENTRY_JSON_TYPE;


public class LinearRegressionTrainFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/linearRegressionTrain");

    static final TypeName INBOX = TypeName.typeNameFromString("pred/blobWrite");

    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(LinearRegressionTrainFn::new)
                    .build();

    private static Logger l;
    private String dataSetType;
    LinearRegressionTrainBatched linearRegressionTrainBatched;
    String datasetName;
    private Properties p;

    public static void initLogger(Logger l_) {
        l = l_;
    }


    public void setup(String dataSetType) {
        p = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get("/resources/all_tasks.properties"))) {
            p.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.dataSetType = dataSetType;
        initLogger(LoggerFactory.getLogger("APP"));
        linearRegressionTrainBatched = new LinearRegressionTrainBatched();
        linearRegressionTrainBatched.setup(l, p);
    }


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        DbEntry dbEntry = message.as(Db_ENTRY_JSON_TYPE);
        setup(dbEntry.getDataSetType());
        String msgid = dbEntry.getMgsid();
        String trainData = dbEntry.getTrainData();
        String rowKeyEnd = dbEntry.getRowKeyEnd();

        HashMap<String, String> map = new HashMap();
        //        obsVal="22.7,49.3,0,1955.22,27"; //dummy
        map.put(AbstractTask.DEFAULT_KEY, trainData);

        if (dataSetType.equals("SYS")) {
            datasetName = p.getProperty("TRAIN.DATASET_NAME_SYS");
        }
        if (dataSetType.equals("TAXI") | dataSetType.equals("FIT")) {
            datasetName = p.getProperty("TRAIN.DATASET_NAME_TAXI");
        }

        String filename = datasetName + "-MLR-" + rowKeyEnd + ".model";
        if (dataSetType.equals("SYS")) {
            filename = datasetName + "-MLR-" + "-1422748810000" + ".model";
        }
        if (dataSetType.equals("TAXI") | dataSetType.equals("FIT")) {
            filename = datasetName + "-MLR-" + "1358102664000.model";
        }

        map.put("FILENAME", filename);

        Float res = linearRegressionTrainBatched.doTask(map);
        if (res != null) {
            if (res != Float.MIN_VALUE) {
                TrainEntry trainEntry = new TrainEntry("model", msgid, rowKeyEnd, "MLR", filename, dbEntry.getDataSetType());
                System.out.println(trainEntry);
                context.send(
                        MessageBuilder.forAddress(INBOX, String.valueOf(trainEntry.getMsgid()))
                                .withCustomType(Train_ENTRY_JSON_TYPE, trainEntry)
                                .build());
            } else {
                if (l.isWarnEnabled()) l.warn("Error in LinearRegressionPredictorBolt");
                throw new RuntimeException();
            }
        }
        return context.done();
    }
}
