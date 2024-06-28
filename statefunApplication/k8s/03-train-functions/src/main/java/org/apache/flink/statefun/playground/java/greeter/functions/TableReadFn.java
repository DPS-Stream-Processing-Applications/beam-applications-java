package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AzureTableRangeQueryTaskFIT;
import org.apache.flink.statefun.playground.java.greeter.tasks.AzureTableRangeQueryTaskGRID;
import org.apache.flink.statefun.playground.java.greeter.tasks.AzureTableRangeQueryTaskSYS;
import org.apache.flink.statefun.playground.java.greeter.tasks.AzureTableRangeQueryTaskTAXI;
import org.apache.flink.statefun.playground.java.greeter.types.DbEntry;
import org.apache.flink.statefun.playground.java.greeter.types.SourceEntry;
import org.apache.flink.statefun.playground.java.greeter.types.azure.FIT_data;
import org.apache.flink.statefun.playground.java.greeter.types.azure.SYS_City;
import org.apache.flink.statefun.playground.java.greeter.types.azure.Taxi_Trip;
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
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.Db_ENTRY_JSON_TYPE;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.Source_ENTRY_JSON_TYPE;


public class TableReadFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/tableRead");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(TableReadFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/annotation");
    static final TypeName INBOX_2 = TypeName.typeNameFromString("pred/linearRegressionTrain");
    private static Logger l;
    private String datatype;
    private Properties p;
    private AzureTableRangeQueryTaskFIT azureTableRangeQueryTaskFIT;
    private AzureTableRangeQueryTaskSYS azureTableRangeQueryTaskSYS;
    private AzureTableRangeQueryTaskGRID azureTableRangeQueryTaskGRID;

    private AzureTableRangeQueryTaskTAXI azureTableRangeQueryTaskTAXI;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static String returnDataTrainData(String datatype) {
        switch (datatype) {
            case "TAXI":
                return "/resources/TAXI_sample_data_senml.csv";
            case "SYS":
                return "/resources/SYS_sample_data_senml.csv";
            case "FIT":
                return "/resources/FIT_sample_data_senml.csv";
            default:
                throw new RuntimeException("Type not recognized");
        }
    }


    public void setup(String datatype) {
        initLogger(LoggerFactory.getLogger("APP"));
        p = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get("/resources/all_tasks.properties"))) {
            p.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.datatype = datatype;
        String trainDataSet = returnDataTrainData(datatype);
        if (datatype.equals("FIT")) {
            azureTableRangeQueryTaskFIT = new AzureTableRangeQueryTaskFIT(trainDataSet);
            azureTableRangeQueryTaskFIT.setup(l, p);
        }
        if (datatype.equals("GRID")) {
            azureTableRangeQueryTaskGRID = new AzureTableRangeQueryTaskGRID(trainDataSet);
            azureTableRangeQueryTaskGRID.setup(l, p);
        }
        if (datatype.equals("SYS")) {
            azureTableRangeQueryTaskSYS = new AzureTableRangeQueryTaskSYS(trainDataSet);
            azureTableRangeQueryTaskSYS.setup(l, p);
        }
        if (datatype.equals("TAXI")) {
            azureTableRangeQueryTaskTAXI = new AzureTableRangeQueryTaskTAXI(trainDataSet);
            azureTableRangeQueryTaskTAXI.setup(l, p);
        }
    }

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        SourceEntry sourceEntry = message.as(Source_ENTRY_JSON_TYPE);
        setup(sourceEntry.getDataSetType());
        String msgId = sourceEntry.getMsgid();
        String ROWKEYSTART = sourceEntry.getRowKeyStart();
        String ROWKEYEND = sourceEntry.getRowKeyEnd();


        HashMap<String, String> map = new HashMap();
        map.put("ROWKEYSTART", ROWKEYSTART);
        map.put("ROWKEYEND", ROWKEYEND);

        StringBuilder bf = new StringBuilder();
        if (Objects.equals(datatype, "FIT")) {
            azureTableRangeQueryTaskFIT.doTaskLogicDummy(map);
            Iterable<FIT_data> result = (Iterable<FIT_data>) azureTableRangeQueryTaskFIT.getLastResult();

            // Loop through the results, displaying information about the entity
            for (FIT_data entity : result) {
                //            if(l.isInfoEnabled())
                //            l.info("partition key {} and
                // fareamount{}",entity.getPartitionKey(),entity.getFare_amount());

                bf.append(entity.getAcc_ankle_x())
                        .append(",")
                        .append(entity.getAcc_ankle_y())
                        .append(",")
                        .append(entity.getAcc_ankle_z())
                        .append(",")
                        .append(entity.getAcc_arm_x())
                        .append(",")
                        .append(entity.getAcc_arm_y())
                        .append(",")
                        .append(entity.getAcc_arm_z())
                        .append(",")
                        .append(entity.getAcc_chest_x())
                        .append(",")
                        .append(entity.getAcc_chest_y())
                        .append(",")
                        .append(entity.getAcc_chest_z())
                        .append(",")
                        .append(entity.getEcg_lead_1())
                        .append("\n");
            }
        } else if (Objects.equals(datatype, "SYS")) {
            azureTableRangeQueryTaskSYS.doTaskLogicDummy(map);

            Iterable<SYS_City> result = (Iterable<SYS_City>) azureTableRangeQueryTaskSYS.getLastResult();

            // Loop through the results, displaying information about the entity
            for (SYS_City entity : result) {
                //            System.out.println(entity.getPartitionKey() + " " +
                // entity.getRangeKey() + "\t" + entity.getAirquality_raw() );
                bf.append(entity.getTemperature())
                        .append(",")
                        .append(entity.getHumidity())
                        .append(",")
                        .append(entity.getLight())
                        .append(",")
                        .append(entity.getDust())
                        .append(",")
                        .append(entity.getAirquality_raw())
                        .append("\n");
            }

        } else if (Objects.equals(datatype, "TAXI")) {
            azureTableRangeQueryTaskTAXI.doTaskLogicDummy(map);

            Iterable<Taxi_Trip> result =
                    (Iterable<Taxi_Trip>) azureTableRangeQueryTaskTAXI.getLastResult();

            for (Taxi_Trip entity : result) {
                //            if(l.isInfoEnabled())
                //            l.info("partition key {} and
                // fareamount{}",entity.getPartitionKey(),entity.getFare_amount());

                bf.append(entity.getTrip_time_in_secs())
                        .append(",")
                        .append(entity.getTrip_distance())
                        .append(",")
                        .append(entity.getFare_amount())
                        .append("\n");
            }
        }

        DbEntry entry = new DbEntry();
        entry.setMgsid(msgId);
        entry.setTrainData(bf.toString());
        entry.setRowKeyEnd(ROWKEYEND);
        entry.setDataSetType(sourceEntry.getDataSetType());
        entry.setArrivalTime(sourceEntry.getArrivalTime());

        context.send(
                MessageBuilder.forAddress(INBOX, String.valueOf(entry.getMgsid()))
                        .withCustomType(Db_ENTRY_JSON_TYPE, entry)
                        .build());
        context.send(
                MessageBuilder.forAddress(INBOX_2, String.valueOf(entry.getMgsid()))
                        .withCustomType(Db_ENTRY_JSON_TYPE, entry)
                        .build());

        return context.done();
    }
}
