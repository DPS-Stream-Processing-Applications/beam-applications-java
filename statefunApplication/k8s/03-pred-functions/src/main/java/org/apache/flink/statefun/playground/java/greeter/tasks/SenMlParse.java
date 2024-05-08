package org.apache.flink.statefun.playground.java.greeter.tasks;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author simmhan, shilpa
 */
public class SenMlParse extends AbstractTask<String, Map> {

    private static final Object SETUP_LOCK = new Object();
    // static fields common to all threads
    private static boolean doneSetup = false;
    private static int useMsgField;
    private final String dataSetType;
    private ArrayList<String> senMLlist;
    private String sampledata;

    private boolean isJson;

    public SenMlParse(String dataSetType, boolean isJson) {
        this.dataSetType = dataSetType;
        this.isJson = isJson;
    }

    public void setup(Logger l_, Properties p_) {
        super.setup(l_, p_);
        synchronized (SETUP_LOCK) {
            if (!doneSetup) {
                useMsgField = Integer.parseInt(p_.getProperty("PARSE.SENML.USE_MSG_FIELD", "0"));
                doneSetup = true;
            }
            sampledata = p_.getProperty("PARSE.SENML.SAMPLEDATA");
        }
    }

    @Override
    protected Float doTaskLogic(Map map) {
        if (!isJson) {
            Map<String, String> output = doTaskLogicCsv(map);
            super.setLastResult(output);
            return 1.0f;
        }
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject;

        try {
            String m;
            if (useMsgField == -1) m = sampledata;
            else m = (String) map.get(AbstractTask.DEFAULT_KEY);
            jsonObject = (JSONObject) jsonParser.parse(m);
            long baseTime = 0L;
            if (dataSetType.equals("TAXI") | dataSetType.equals("SYS")) {
                baseTime =
                        (long) (jsonObject.get("bt") == null ? 0L : jsonObject.get("bt"));
            }
            if (dataSetType.equals("FIT")) {
                baseTime = Long.parseLong(((String) jsonObject.get("bt")));
            }

            String baseUnit = (String) ((jsonObject.get("bu") == null) ? null : jsonObject.get("bu"));
            String baseName = (String) ((jsonObject.get("bn") == null) ? null : jsonObject.get("bn"));
            JSONArray jsonArr = (JSONArray) jsonObject.get("e");
            Object v;
            String n, u;
            long t;
            HashMap mapkeyValues = new HashMap<String, String>();
            mapkeyValues.put("timestamp", String.valueOf(baseTime));
            for (int j = 0; j < jsonArr.size(); j++) {
                jsonObject = (JSONObject) jsonArr.get(j);

                v =
                        (jsonObject.get("v") == null)
                                ? (String) jsonObject.get("vs")
                                : (String) jsonObject.get("v");

                t = (jsonObject.get("t") == null) ? 0 : (long) jsonObject.get("t");

                t = t + baseTime;

                /* if name does not exist, consider base name */
                n = (jsonObject.get("n") == null) ? baseName : (String) jsonObject.get("n");

                u = (jsonObject.get("u") == null) ? baseUnit : (String) jsonObject.get("u");

                /* Add to  Hashmap  each key value pair */
                mapkeyValues.put(n, v);
            }
            super.setLastResult(mapkeyValues);
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    protected Map<String, String> doTaskLogicCsv(Map map) {
        HashMap<String, String> mapkeyValues = new HashMap<>();
        try {
            String m;
            if (useMsgField == -1) m = sampledata;
            else m = (String) map.get(AbstractTask.DEFAULT_KEY);

            String[] inputData = m.split(",");
            /*this is for TAXI dataset*/
            long baseTime = 0L;
            if (dataSetType.equals("TAXI") | dataSetType.equals("SYS")) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                LocalDateTime dateTime = LocalDateTime.parse(inputData[1], formatter);

                baseTime = dateTime.toEpochSecond(ZoneOffset.UTC);
            }
            if (dataSetType.equals("FIT")) {
                baseTime = 0L;
                // dataset
            }

            if (dataSetType.equals("TAXI")) {
                mapkeyValues.put("timestamp", String.valueOf(baseTime));
                mapkeyValues.put("taxi_identifier", inputData[0]);
                mapkeyValues.put("pickup_datetime", inputData[1]);
                mapkeyValues.put("pickup_longitude", inputData[5]);
                mapkeyValues.put("pickup_latitude", inputData[6]);
                mapkeyValues.put("dropoff_longitude", inputData[7]);
                mapkeyValues.put("dropoff_latitude", inputData[8]);
                mapkeyValues.put("payment_type", inputData[9]);
                mapkeyValues.put("hack_license", inputData[0]);
                mapkeyValues.put("dropoff_datetime", inputData[2]);
                mapkeyValues.put("trip_time_in_secs", inputData[3]);
                mapkeyValues.put("trip_distance", inputData[4]);
                mapkeyValues.put("fare_amount", inputData[10]);
                mapkeyValues.put("surcharge", inputData[11]);
                mapkeyValues.put("mta_tax", inputData[12]);
                mapkeyValues.put("tip_amount", inputData[13]);
                mapkeyValues.put("tolls_amount", inputData[14]);
                mapkeyValues.put("total_amount", inputData[15]);
            } else if (dataSetType.equals("FIT")) {
                mapkeyValues.put("timestamp", String.valueOf(baseTime));
                mapkeyValues.put("subjectId", inputData[0]);
                mapkeyValues.put("acc_chest_x", inputData[1]);
                mapkeyValues.put("acc_chest_y", inputData[2]);
                mapkeyValues.put("acc_chest_z", inputData[3]);

                mapkeyValues.put("ecg_lead_1", inputData[4]);
                mapkeyValues.put("ecg_lead_2", inputData[5]);

                mapkeyValues.put("acc_ankle_x", inputData[6]);
                mapkeyValues.put("acc_ankle_y", inputData[7]);
                mapkeyValues.put("acc_ankle_z", inputData[8]);

                mapkeyValues.put("gyro_ankle_x", inputData[9]);
                mapkeyValues.put("gyro_ankle_y", inputData[10]);
                mapkeyValues.put("gyro_ankle_z", inputData[11]);

                mapkeyValues.put("magnetometer_ankle_x", inputData[12]);
                mapkeyValues.put("magnetometer_ankle_y", inputData[13]);
                mapkeyValues.put("magnetometer_ankle_z", inputData[14]);

                mapkeyValues.put("acc_arm_x", inputData[15]);
                mapkeyValues.put("acc_arm_y", inputData[16]);
                mapkeyValues.put("acc_arm_z", inputData[17]);

                mapkeyValues.put("gyro_arm_x", inputData[18]);
                mapkeyValues.put("gyro_arm_y", inputData[19]);
                mapkeyValues.put("gyro_arm_z", inputData[20]);

                mapkeyValues.put("magnetometer_arm_x", inputData[21]);
                mapkeyValues.put("magnetometer_arm_y", inputData[22]);
                mapkeyValues.put("magnetometer_arm_z", inputData[23]);

                mapkeyValues.put("label", inputData[24]);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return mapkeyValues;
    }
}
