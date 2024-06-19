package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.ReadFromDatabaseTask;
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

    private static Logger l;

    SenMlParse senMLParseTask;
    private static final Object DATABASE_LOCK = new Object();
    private String dataSetType;

    private Properties p;
    private ArrayList<String> observableFields;
    private String[] metaFields;
    private String idField;

    private boolean isJson;

    private ReadFromDatabaseTask readFromDatabaseTask;

    private String connectionUrl;

    private String dataBaseName;
    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/senmlParse");

    static final TypeName INBOX = TypeName.typeNameFromString("pred/decisionTree");

    static final TypeName INBOX_2 = TypeName.typeNameFromString("pred/linearRegression");

    static final TypeName INBOX_3 = TypeName.typeNameFromString("pred/average");

    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(ParsePredictFn::new)
                    .build();

    public static void initLogger(Logger l_) {
        l = l_;
    }


    public void setup(String dataSetType, String connectionUrl, String dataBaseName) {
        p = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get("/resources/all_tasks.properties"))) {
            p.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String line = "";

        try {
            initLogger(LoggerFactory.getLogger("APP"));
            senMLParseTask = new SenMlParse(dataSetType, true);
            senMLParseTask.setup(l, p);
            observableFields = new ArrayList<>();
            /*
            readFromDatabaseTask = new ReadFromDatabaseTask(connectionUrl, dataBaseName);
            synchronized (DATABASE_LOCK) {
                readFromDatabaseTask.setup(l, p);
            }

             */
            ArrayList<String> metaList = new ArrayList<>();

            String meta;
            byte[] csvContent;
            if (dataSetType.equals("TAXI")) {
                idField = p.getProperty("PARSE.ID_FIELD_SCHEMA_TAXI");
                /*
                HashMap<String, String> map = new HashMap<>();
                map.put("fileName", "taxi-schema-without-annotation_csv");
                synchronized (DATABASE_LOCK) {
                    readFromDatabaseTask.doTask(map);
                }
                csvContent = readFromDatabaseTask.getLastResult();
                */

                line = "taxi_identifier,hack_license,pickup_datetime,timestamp,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount";
                meta = p.getProperty("PARSE.META_FIELD_SCHEMA_TAXI");

            } else if (dataSetType.equals("SYS")) {
                idField = p.getProperty("PARSE.ID_FIELD_SCHEMA_SYS");
                HashMap<String, String> map = new HashMap<>();
                map.put("fileName", "sys-schema_without_annotationfields_txt");
                synchronized (DATABASE_LOCK) {
                    readFromDatabaseTask.doTask(map);
                }
                csvContent = readFromDatabaseTask.getLastResult();
                meta = p.getProperty("PARSE.META_FIELD_SCHEMA_SYS");
            } else if (dataSetType.equals("FIT")) {
                idField = p.getProperty("PARSE.ID_FIELD_SCHEMA_FIT");
                HashMap<String, String> map = new HashMap<>();
                map.put("fileName", "mhealth_schema_csv");
                synchronized (DATABASE_LOCK) {
                    readFromDatabaseTask.doTask(map);
                }
                csvContent = readFromDatabaseTask.getLastResult();
                meta = p.getProperty("PARSE.META_FIELD_SCHEMA_FIT");
            } else {
                throw new IllegalArgumentException("Invalid dataSetType: " + dataSetType);
            }

            /*
            ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

             */

            metaFields = meta.split(",");
            for (String metaField : metaFields) {
                metaList.add(metaField);
            }

            //String line = br.readLine();

            String[] obsType = line.split(",");
            for (String field : obsType) {
                if (!metaList.contains(field)) {
                    observableFields.add(field);
                }
            }

            //br.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error when setting up ParsePredictBeam: " + e);
        }
    }

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        try {
            SourceEntry sourceEntry = message.as(SOURCE_ENTRY_JSON_TYPE);
            String ipAddress = System.getenv("MONGO_DB_ADDRESS");
            setup(sourceEntry.getDataSetType(),ipAddress, "mydb");
            String msg = sourceEntry.getPayload();
            String msgId = String.valueOf(sourceEntry.getMsgid());

            HashMap<String, String> map = new HashMap<>();
            map.put(AbstractTask.DEFAULT_KEY, msg);

            senMLParseTask.doTask(map);
            HashMap<String, String> resultMap = (HashMap) senMLParseTask.getLastResult();

            StringBuilder meta = new StringBuilder();
            StringBuilder obsVal = new StringBuilder();
            for (int i = 0; i < metaFields.length; i++) {
                meta.append(resultMap.get((metaFields[i]))).append(",");
            }
            meta = meta.deleteCharAt(meta.lastIndexOf(","));
            for (int j = 0; j < observableFields.size(); j++) {
                obsVal.append(resultMap.get(observableFields.get(j)));
                obsVal.append(",");
            }
            obsVal = obsVal.deleteCharAt(obsVal.lastIndexOf(","));

             /*
            StringBuilder obsVal = new StringBuilder();
            StringBuilder meta = new StringBuilder();
            obsVal.append("2013003495,2013003088,320,2.0,5.5,0.5,0.5,1.95,0.0,8.45");
            meta.append("2013-01-14  00:00:16,1358121616,-73.970146,40.756584,-73.951225,40.782761,CRD");

              */
            SenMlEntry senMlEntry =
                    new SenMlEntry(
                            msgId,
                            resultMap.get(idField),
                            meta.toString(),
                            "dummyobsType",
                            obsVal.toString(),
                            "MSGTYPE",
                            "DumbType", sourceEntry.getDataSetType());

            context.send(
                    MessageBuilder.forAddress(INBOX, String.valueOf(senMlEntry.getMsgid()))
                            .withCustomType(SENML_ENTRY_JSON_TYPE, senMlEntry)
                            .build());
            context.send(
                    MessageBuilder.forAddress(INBOX_2, String.valueOf(senMlEntry.getMsgid()))
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
