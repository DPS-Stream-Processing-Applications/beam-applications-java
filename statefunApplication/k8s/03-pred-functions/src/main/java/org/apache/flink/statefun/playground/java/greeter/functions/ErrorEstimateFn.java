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
    private static Logger l;
    private String dataSetType;
    private Properties p;
    private String Res = "0";
    private String avgRes = "0";

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
        this.dataSetType = dataSetType;
        initLogger(LoggerFactory.getLogger("APP"));
    }

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        try {
            long arrivalTime;
            if (message.is(LINEAR_REGRESSION_ENTRY_JSON_TYPE)) {
                LinearRegressionEntry linearRegressionEntry = message.as(LINEAR_REGRESSION_ENTRY_JSON_TYPE);
                setup(linearRegressionEntry.getDataSetType());
                String msgId = linearRegressionEntry.getMsgid();
                String analyticsType = linearRegressionEntry.getAnalyticType();

                String sensorMeta = linearRegressionEntry.getMeta();
                String obsVal = linearRegressionEntry.getObsval();
                if (analyticsType.equals("MLR")) {
                    Res = linearRegressionEntry.getRes();
                }

                if (l.isInfoEnabled()) l.info("analyticsType:{},Res:{},avgRes:{}", analyticsType, Res, avgRes);

                if (analyticsType.equals("MLR")) {
                    float errval = 0;
                    String fareString;
                    String[] obsValSplit = obsVal.split(",");
                    if (dataSetType.equals("TAXI")) {
                        if (obsValSplit.length > 3) {
                            fareString = obsValSplit[4];
                        } else {
                            fareString = obsValSplit[2];
                        }

                        float fare_amount = Float.parseFloat(fareString);
                        errval = (fare_amount - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
                    }
                    if (dataSetType.equals("SYS")) {
                        float air_quality = Float.parseFloat((linearRegressionEntry.getObsval()).split(",")[4]);
                        errval = (air_quality - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
                    }

                    if (dataSetType.equals("FIT")) {
                        float fare_amount = Float.parseFloat((linearRegressionEntry.getObsval().split(",")[2]));
                        errval = (fare_amount - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
                    }
                    ErrorEstimateEntry errorEstimateEntry = new ErrorEstimateEntry(sensorMeta, errval, msgId, analyticsType, obsVal, linearRegressionEntry.getDataSetType());
                    errorEstimateEntry.setArrivalTime(linearRegressionEntry.getArrivalTime());
                    context.send(
                            MessageBuilder.forAddress(INBOX, String.valueOf(errorEstimateEntry.getMsgid()))
                                    .withCustomType(ERROR_ESTIMATE_ENTRY_JSON_TYPE, errorEstimateEntry)
                                    .build());

                }
            } else if (message.is(AVERAGE_ENTRY_JSON_TYPE)) {
                AverageEntry averageEntry = message.as(AVERAGE_ENTRY_JSON_TYPE);
                setup(averageEntry.getDataSetType());
                arrivalTime = averageEntry.getArrivalTime();
                String msgId = averageEntry.getMsgid();
                String analyticsType = averageEntry.getAnalyticType();
                String sensorMeta = averageEntry.getMeta();
                String obsVal = averageEntry.getObsVal();

                if (analyticsType.equals("AVG")) {
                    avgRes = averageEntry.getAvGres();
                }

                if (l.isInfoEnabled()) l.info("analyticsType:{},Res:{},avgRes:{}", analyticsType, Res, avgRes);

                if (analyticsType.equals("MLR")) {
                    float errval = 0;
                    if (dataSetType.equals("TAXI")) {
                        float fare_amount = Float.parseFloat((obsVal).split(",")[2]);
                        errval = (fare_amount - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
                    }
                    if (dataSetType.equals("SYS")) {
                        float air_quality = Float.parseFloat((averageEntry.getObsVal()).split(",")[4]);
                        errval = (air_quality - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
                    }
                    if (dataSetType.equals("FIT")) {
                        float fare_amount = Float.parseFloat((averageEntry.getObsVal()).split(",")[7]);
                        errval = (fare_amount - Float.parseFloat(Res)) / Float.parseFloat(avgRes);
                    }
                    if (l.isInfoEnabled()) l.info(("errval -" + errval));
                    ErrorEstimateEntry errorEstimateEntry = new ErrorEstimateEntry(sensorMeta, errval, msgId, analyticsType, obsVal, averageEntry.getDataSetType());
                    errorEstimateEntry.setArrivalTime(arrivalTime);
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
