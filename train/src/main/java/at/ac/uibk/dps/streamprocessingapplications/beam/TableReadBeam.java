package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.DbEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.SourceEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.FIT_data;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.SYS_City;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.Taxi_Trip;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AzureTableRangeQueryTaskFIT;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AzureTableRangeQueryTaskGRID;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AzureTableRangeQueryTaskSYS;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AzureTableRangeQueryTaskTAXI;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;

public class TableReadBeam extends DoFn<SourceEntry, DbEntry> {
    private Properties p;
    private String datatype;

    private AzureTableRangeQueryTaskFIT azureTableRangeQueryTaskFIT;

    private AzureTableRangeQueryTaskSYS azureTableRangeQueryTaskSYS;

    private AzureTableRangeQueryTaskGRID azureTableRangeQueryTaskGRID;

    private AzureTableRangeQueryTaskTAXI azureTableRangeQueryTaskTAXI;

    // private TupleTag<String> trainData = new TupleTag<>();
    // private TupleTag<String> msgId = new TupleTag<>();
    // private TupleTag<String> rowKeyEnd = new TupleTag<>();

    private static Logger l;

    public TableReadBeam(Properties p_, String outCSVFileName, String datatype) {
        this.p = p_;
        this.datatype = datatype;
    }

    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Setup
    public void setup() {
        azureTableRangeQueryTaskFIT = new AzureTableRangeQueryTaskFIT();
        initLogger(LoggerFactory.getLogger("APP"));
        azureTableRangeQueryTaskFIT.setup(l, p);
        azureTableRangeQueryTaskGRID = new AzureTableRangeQueryTaskGRID();
        azureTableRangeQueryTaskGRID.setup(l, p);
        azureTableRangeQueryTaskSYS = new AzureTableRangeQueryTaskSYS();
        azureTableRangeQueryTaskSYS.setup(l, p);
        azureTableRangeQueryTaskTAXI = new AzureTableRangeQueryTaskTAXI();
        azureTableRangeQueryTaskTAXI.setup(l, p);
    }

    @ProcessElement
    public void processElement(@Element SourceEntry input, OutputReceiver<DbEntry> out) {

        String msgId = input.getMsgid();
        String ROWKEYSTART = input.getRowKeyStart();
        String ROWKEYEND = input.getRowKeyEnd();

        if (l.isInfoEnabled()) l.info("ROWKEYSTART:{} ROWKEYEND{}", ROWKEYSTART, ROWKEYEND);

        HashMap<String, String> map = new HashMap();
        map.put("ROWKEYSTART", ROWKEYSTART);
        map.put("ROWKEYEND", ROWKEYEND);

        StringBuilder bf = new StringBuilder();
        if (Objects.equals(datatype, "FIT")) {
            azureTableRangeQueryTaskFIT.doTaskLogicDummy(map);
            Iterable<FIT_data> result =
                    (Iterable<FIT_data>) azureTableRangeQueryTaskFIT.getLastResult();

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

            Iterable<SYS_City> result =
                    (Iterable<SYS_City>) azureTableRangeQueryTaskSYS.getLastResult();

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

            // Loop through the results, displaying information about the entity
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
        //        if(l.isInfoEnabled()) {
        //            stopwatch.stop(); // optional
        //            l.info("Time elapsed for azureTableRangeQueryTask() is {}",
        // stopwatch.elapsed(MILLISECONDS)); //
        //        }
        if (l.isInfoEnabled()) l.info("data for annotation {}", bf);
        String outputString = bf + "," + msgId + "," + ROWKEYEND;
        DbEntry entry = new DbEntry();
        entry.setMgsid(msgId);
        entry.setTrainData(bf.toString());
        entry.setRowKeyEnd(ROWKEYEND);
        out.output(entry);
    }
}
