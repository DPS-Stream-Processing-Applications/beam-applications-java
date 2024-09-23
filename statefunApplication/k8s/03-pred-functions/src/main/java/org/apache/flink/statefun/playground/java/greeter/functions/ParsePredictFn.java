package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.SenMlParse;
import org.apache.flink.statefun.playground.java.greeter.types.generated.SenMlEntry;
import org.apache.flink.statefun.playground.java.greeter.types.generated.SourceEntry;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.SEN_ML_ENTRY_PROTOBUF_TYPE;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.SOURCE_ENTRY_PROTOBUF_TYPE;

public class ParsePredictFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/senmlParse");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(ParsePredictFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/decisionTree");
    static final TypeName INBOX_2 = TypeName.typeNameFromString("pred/linearRegression");
    static final TypeName INBOX_3 = TypeName.typeNameFromString("pred/average");


    private  Logger l;
    SenMlParse senMLParseTask;
    private ArrayList<String> observableFields;
    private String[] metaFields;
    private String idField;



    public  void initLogger(Logger l_) {
        this.l = l_;
    }


    public void setup(String dataSetType) {
        Properties p = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get("/resources/all_tasks.properties"))) {
            p.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String line;

        try {
            initLogger(LoggerFactory.getLogger("APP"));
            senMLParseTask = new SenMlParse(dataSetType, true);
            int useField = Integer.parseInt(p.getProperty("PARSE.SENML.USE_MSG_FIELD", "0"));
            String sampleData = p.getProperty("PARSE.SENML.SAMPLEDATA");
            senMLParseTask.setup(l, sampleData, useField);
            observableFields = new ArrayList<>();
            ArrayList<String> metaList = new ArrayList<>();

            String meta;
            switch (dataSetType) {
                case "TAXI":
                    idField = p.getProperty("PARSE.ID_FIELD_SCHEMA_TAXI");
                    line = "taxi_identifier,hack_license,pickup_datetime,timestamp,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount";
                    meta = p.getProperty("PARSE.META_FIELD_SCHEMA_TAXI");

                    break;
                case "SYS":
                    idField = p.getProperty("PARSE.ID_FIELD_SCHEMA_SYS");
                    line = "timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw";
                    meta = p.getProperty("PARSE.META_FIELD_SCHEMA_SYS");
                    break;
                case "FIT":
                    idField = p.getProperty("PARSE.ID_FIELD_SCHEMA_FIT");
                    line = "subjectId,timestamp,acc_chest_x,acc_chest_y,acc_chest_z,ecg_lead_1,ecg_lead_2,acc_ankle_x,"
                            + "acc_ankle_y,acc_ankle_z,gyro_ankle_x,gyro_ankle_y,gyro_ankle_z,magnetometer_ankle_x,"
                            + "magnetometer_ankle_y,magnetometer_ankle_z,acc_arm_x,acc_arm_y,acc_arm_z,gyro_arm_x,gyro_"
                            + "arm_y,gyro_arm_z,magnetometer_arm_x,magnetometer_arm_y,magnetometer_arm_z,label";
                    meta = p.getProperty("PARSE.META_FIELD_SCHEMA_FIT");
                    break;
                default:
                    throw new IllegalArgumentException("Invalid dataSetType: " + dataSetType);
            }
            p.clear();
            p = null;
            metaFields = meta.split(",");
            Collections.addAll(metaList, metaFields);

            String[] obsType = line.split(",");
            for (String field : obsType) {
                if (!metaList.contains(field)) {
                    observableFields.add(field);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error when setting up ParsePredictBeam: " + e);
        }
    }



    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        try {
            SourceEntry sourceEntry = message.as(SOURCE_ENTRY_PROTOBUF_TYPE);
            setup(sourceEntry.getDataSetType());
            String msg = sourceEntry.getPayload();
            String msgId = String.valueOf(sourceEntry.getMsgid());

            HashMap<String, String> map = new HashMap<>();
            map.put(AbstractTask.DEFAULT_KEY, msg);


            senMLParseTask.doTask(map);
            HashMap<String, String> resultMap = (HashMap) senMLParseTask.getLastResult();

            StringBuilder meta = new StringBuilder();
            StringBuilder obsVal = new StringBuilder();
            for (String metaField : metaFields) {
                meta.append(resultMap.get(metaField)).append(",");
            }
            meta = meta.deleteCharAt(meta.lastIndexOf(","));
            for (String observableField : observableFields) {
                obsVal.append(resultMap.get(observableField));
                obsVal.append(",");
            }
            obsVal = obsVal.deleteCharAt(obsVal.lastIndexOf(","));



            final SenMlEntry senMlEntry =
                    SenMlEntry.newBuilder()
                            .setMsgid(msgId)
                            .setSensorID(resultMap.get(idField))
                            .setMeta(meta.toString())
                            .setObsType("dummyobsType")
                            .setObsVal(String.valueOf(obsVal))
                            .setMsgtype("MSGTYPE")
                            .setAnalyticType("DumbType")
                            .setDataSetType(sourceEntry.getDataSetType()).build();



            meta=null;
            obsVal=null;

            //senMlEntry.setArrivalTime(sourceEntry.getArrivalTime());

            context.send(
                    MessageBuilder.forAddress(INBOX_2, String.valueOf(senMlEntry.getMsgid()))
                            .withCustomType(SEN_ML_ENTRY_PROTOBUF_TYPE, senMlEntry)
                            .build());


            context.send(
                    MessageBuilder.forAddress(INBOX, String.valueOf(senMlEntry.getMsgid()))
                            .withCustomType(SEN_ML_ENTRY_PROTOBUF_TYPE, senMlEntry)
                            .build());




            context.send(
                    MessageBuilder.forAddress(INBOX_3, String.valueOf(senMlEntry.getMsgid()))
                            .withCustomType(SEN_ML_ENTRY_PROTOBUF_TYPE, senMlEntry)
                            .build());

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error in ParsePredictBeam " + e);
        }
        observableFields=null;
        metaFields=null;
        senMLParseTask.tearDown();
        return context.done();
    }
}
