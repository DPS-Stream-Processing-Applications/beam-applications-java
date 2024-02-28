package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.DbEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.FIT_data;
import at.ac.uibk.dps.streamprocessingapplications.entity.SourceEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AzureTableRangeQueryTaskFIT;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableReadBeam extends DoFn<SourceEntry, DbEntry> {
    private Properties p;

    private AzureTableRangeQueryTaskFIT azureTableRangeQueryTaskFIT;

    // private TupleTag<String> trainData = new TupleTag<>();
    // private TupleTag<String> msgId = new TupleTag<>();
    // private TupleTag<String> rowKeyEnd = new TupleTag<>();

    private static Logger l;

    public TableReadBeam(Properties p_) {
        this.p = p_;
    }

    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Setup
    public void setup() {
        azureTableRangeQueryTaskFIT = new AzureTableRangeQueryTaskFIT();
        initLogger(LoggerFactory.getLogger("APP"));
        azureTableRangeQueryTaskFIT.setup(l, p);
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

        //        Stopwatch stopwatch=null;
        //        if(l.isInfoEnabled()) {
        //            stopwatch = Stopwatch.createStarted(); //
        //        }

        azureTableRangeQueryTaskFIT.doTaskLogicDummy(map);
        Iterable<FIT_data> result =
                (Iterable<FIT_data>) azureTableRangeQueryTaskFIT.getLastResult();

        StringBuilder bf = new StringBuilder();
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
