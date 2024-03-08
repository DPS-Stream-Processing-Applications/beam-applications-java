package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.SenMlEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.SourceEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.SenMlParse;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParsePredictBeam extends DoFn<SourceEntry, SenMlEntry> {

    private Properties p;

    public ParsePredictBeam(Properties p_) {
        p = p_;
    }

    private static Logger l;

    public static void initLogger(Logger l_) {
        l = l_;
    }

    private ArrayList<String> observableFields;
    private String[] metaFields;
    private String idField;
    SenMlParse senMLParseTask;

    @Setup
    public void setup() {

        try {
            initLogger(LoggerFactory.getLogger("APP"));
            senMLParseTask = new SenMlParse();
            senMLParseTask.setup(l, p);
            observableFields = new ArrayList();
            String line;
            ArrayList<String> metaList = new ArrayList<String>();

            /* read meta field list from property */
            String meta = p.getProperty("PARSE.META_FIELD_SCHEMA");
            idField = p.getProperty("PARSE.ID_FIELD_SCHEMA");
            metaFields = meta.split(",");
            for (int i = 0; i < metaFields.length; i++) {
                metaList.add(metaFields[i]);
            }
            /* read csv schema to read fields observable into a list
            excluding meta fields read above */
            FileReader reader = new FileReader(p.getProperty("PARSE.CSV_SCHEMA_FILEPATH"));
            BufferedReader br = new BufferedReader(reader);
            line = br.readLine();
            String[] obsType = line.split(",");
            for (int i = 0; i < obsType.length; i++) {
                if (metaList.contains(obsType[i]) == false) {
                    observableFields.add(obsType[i]);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // sample from RowString field
    //    msgId,timestamp,
    // source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
    //    1,1443033000,      ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26

    // Actual TAXI sample data
    //    0                 1           2               3           4                   5
    // 6                   7               8                   9           10                  11
    //     12       13      14         15          16
    // taxi_identifier,hack_license,pickup_datetime,timestamp,trip_time_in_secs,
    // trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type    ,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount
    // 024BE2DFD1B98AF1EA941DEDA63A15CB,9F5FE566E3EE57B85B723B71E370154C,2013-01-14
    // 03:57:00,2013-01-14
    // 04:23:00,1560,19.36,-73.953178,40.776016,-73.779190,40.645145,CRD,52.00,0.00,0.50,13.00,4.80,70.30

    @ProcessElement
    public void processElement(@Element SourceEntry input, OutputReceiver<SenMlEntry> out)
            throws IOException {
        try {
            String msg = input.getPayLoad();

            String msgId = input.getMsgid();
            HashMap<String, String> map = new HashMap();
            map.put(AbstractTask.DEFAULT_KEY, msg);
            senMLParseTask.doTask(map);
            HashMap<String, String> resultMap = (HashMap) senMLParseTask.getLastResult();

            /* loop over to concatenate different meta fields together
             * preserving ordering among them */
            StringBuilder meta = new StringBuilder();
            StringBuilder obsVal = new StringBuilder();
            for (int i = 0; i < metaFields.length; i++) {
                meta.append(resultMap.get((metaFields[i]))).append(",");
            }
            meta = meta.deleteCharAt(meta.lastIndexOf(","));
            for (int j = 0; j < observableFields.size(); j++) {
                obsVal.append((String) resultMap.get((String) observableFields.get(j)));
                obsVal.append(",");
            }
            // obsVal.substring(0, obsVal.length() - 1);

            out.output(
                    new SenMlEntry(
                            msgId,
                            resultMap.get(idField),
                            meta.toString(),
                            "dummyobsType",
                            obsVal.toString(),
                            "MSGTYPE",
                            "DumbType"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Teardown
    public void cleanup() {
        // xmlParse.tearDown();
    }
}
