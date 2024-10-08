package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.SenMlEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.SourceEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.SenMlParse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParsePredictBeam extends DoFn<SourceEntry, SenMlEntry> {

  private static Logger l;
  private final String dataSetType;
  SenMlParse senMLParseTask;
  private Properties p;
  private ArrayList<String> observableFields;
  private String[] metaFields;
  private String idField;

  private boolean isJson;

  public ParsePredictBeam(Properties p_, String dataSetType, boolean isJson) {
    p = p_;
    this.dataSetType = dataSetType;
    this.isJson = isJson;
  }

  public static void initLogger(Logger l_) {
    l = l_;
  }

  @Setup
  public void setup() {
    try {
      initLogger(LoggerFactory.getLogger("APP"));
      senMLParseTask = new SenMlParse(dataSetType, isJson);
      senMLParseTask.setup(l, p);
      observableFields = new ArrayList<>();
      ArrayList<String> metaList = new ArrayList<>();

      String meta;
      String line;
      if (dataSetType.equals("TAXI")) {
        idField = p.getProperty("PARSE.ID_FIELD_SCHEMA_TAXI");
        line =
            "taxi_identifier,hack_license,pickup_datetime,timestamp,trip_time_in_secs,trip_distance,"
                + "pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,"
                + "surcharge,mta_tax,tip_amount,tolls_amount,total_amount";
        meta = p.getProperty("PARSE.META_FIELD_SCHEMA_TAXI");
      } else if (dataSetType.equals("SYS")) {
        idField = p.getProperty("PARSE.ID_FIELD_SCHEMA_SYS");
        line = "timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw";
        meta = p.getProperty("PARSE.META_FIELD_SCHEMA_SYS");
      } else if (dataSetType.equals("FIT")) {
        idField = p.getProperty("PARSE.ID_FIELD_SCHEMA_FIT");
        line =
            "subjectId,timestamp,acc_chest_x,acc_chest_y,acc_chest_z,ecg_lead_1,ecg_lead_2,acc_ankle_x,"
                + "acc_ankle_y,acc_ankle_z,gyro_ankle_x,gyro_ankle_y,gyro_ankle_z,magnetometer_ankle_x,"
                + "magnetometer_ankle_y,magnetometer_ankle_z,acc_arm_x,acc_arm_y,acc_arm_z,gyro_arm_x,gyro_"
                + "arm_y,gyro_arm_z,magnetometer_arm_x,magnetometer_arm_y,magnetometer_arm_z,label";
        meta = p.getProperty("PARSE.META_FIELD_SCHEMA_FIT");
      } else {
        throw new IllegalArgumentException("Invalid dataSetType: " + dataSetType);
      }

      /* read meta field list from property */
      metaFields = meta.split(",");
      for (String metaField : metaFields) {
        metaList.add(metaField);
      }

      /* read csv schema to read fields observable into a list
      excluding meta fields read above */
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

  // sample from RowString field
  //    msgId,timestamp,
  // source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
  //    1,1443033000,      ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26

  // Actual TAXI sample data
  //    0                 1           2               3           4                   5
  // 6                   7               8                   9           10                  11
  //     12       13      14         15          16
  // taxi_identifier,hack_license,pickup_datetime,timestamp,trip_time_in_secs,
  // trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type
  //   ,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount
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
        obsVal.append(resultMap.get(observableFields.get(j)));
        obsVal.append(",");
      }
      obsVal = obsVal.deleteCharAt(obsVal.lastIndexOf(","));
      // obsVal.substring(0, obsVal.length() - 1);

      SenMlEntry senMlEntry =
          new SenMlEntry(
              msgId,
              resultMap.get(idField),
              meta.toString(),
              "dummyobsType",
              obsVal.toString(),
              "MSGTYPE",
              "DumbType");
      senMlEntry.setArrivalTime(input.getArrivalTime());
      out.output(senMlEntry);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Error in ParsePredictBeam " + e);
    }
  }

  @Teardown
  public void cleanup() {
    // xmlParse.tearDown();
  }
}
