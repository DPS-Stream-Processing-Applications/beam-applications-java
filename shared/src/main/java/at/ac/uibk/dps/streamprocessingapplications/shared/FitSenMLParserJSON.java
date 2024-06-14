package at.ac.uibk.dps.streamprocessingapplications.shared;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class FitSenMLParserJSON {
  public static FitnessMeasurements parseSenMLPack(String senMLJsonArray) {
    JSONArray recordPack = new JSONArray(senMLJsonArray);
    if (recordPack.isEmpty()) {
      return new FitnessMeasurements();
    }

    // NOTE: Acquiring the base name from the first entry to add it to each record.
    String baseName = null;
    try {
      baseName = recordPack.getJSONObject(0).getString("bn");
    } catch (JSONException ignored) {
    }

    // NOTE: Variable used in lambda should be final or effectively final.
    final String finalBaseName = baseName;
    Map<String, String> recordNameToRecordString =
        StreamSupport.stream(
                /* WARN:
                 * Use `.iterator()` instead of `.toList()` as `.toList()` appears to not return a `JSON` formatted
                 * string if `.toString()` is applied to the resulting list elements.
                 */
                Spliterators.spliteratorUnknownSize(recordPack.iterator(), Spliterator.ORDERED),
                false)
            .map( // Convert record json string to `JSONObject`
                json -> {
                  JSONObject jsonObject = new JSONObject(json.toString());
                  if (finalBaseName != null && !jsonObject.has("bn")) {
                    jsonObject.put("bn", finalBaseName);
                  }
                  return jsonObject;
                })
            .collect(
                Collectors.toMap(jsonObject -> jsonObject.getString("n"), JSONObject::toString));

    return buildFitnessMeasurementsFromMap(recordNameToRecordString);
  }

  private static FitnessMeasurements buildFitnessMeasurementsFromMap(
      Map<String, String> recordNameToRecordString) {

    /*
    this.label = new SenMLRecordDouble(null, "label", "number", null, null);

     */

    String subjectIdString = recordNameToRecordString.get("subjectId");

    String chestAccelerationXString = recordNameToRecordString.get("acc_chest_x");
    String chestAccelerationYString = recordNameToRecordString.get("acc_chest_y");
    String chestAccelerationZString = recordNameToRecordString.get("acc_chest_z");

    String ECGLead1String = recordNameToRecordString.get("ecg_lead_1");
    String ECGLead2String = recordNameToRecordString.get("ecg_lead_2");

    String ankleAccelerationXString = recordNameToRecordString.get("acc_ankle_x");
    String ankleAccelerationYString = recordNameToRecordString.get("acc_ankle_y");
    String ankleAccelerationZString = recordNameToRecordString.get("acc_ankle_z");
    String ankleGyroXString = recordNameToRecordString.get("gyro_ankle_x");
    String ankleGyroYString = recordNameToRecordString.get("gyro_ankle_y");
    String ankleGyroZString = recordNameToRecordString.get("gyro_ankle_z");
    String ankleMagnetometerXString = recordNameToRecordString.get("magnetometer_ankle_x");
    String ankleMagnetometerYString = recordNameToRecordString.get("magnetometer_ankle_y");
    String ankleMagnetometerZString = recordNameToRecordString.get("magnetometer_ankle_z");

    String armAccelerationXString = recordNameToRecordString.get("acc_arm_x");
    String armAccelerationYString = recordNameToRecordString.get("acc_arm_y");
    String armAccelerationZString = recordNameToRecordString.get("acc_arm_z");
    String armGyroXString = recordNameToRecordString.get("gyro_arm_x");
    String armGyroYString = recordNameToRecordString.get("gyro_arm_y");
    String armGyroZString = recordNameToRecordString.get("gyro_arm_z");
    String armMagnetometerXString = recordNameToRecordString.get("magnetometer_arm_x");
    String armMagnetometerYString = recordNameToRecordString.get("magnetometer_arm_y");
    String armMagnetometerZString = recordNameToRecordString.get("magnetometer_arm_z");

    String labelString = recordNameToRecordString.get("label");

    return new FitnessMeasurements(
        subjectIdString != null ? SenMLParserJSON.parseWithVS(subjectIdString) : null,
        chestAccelerationXString != null
            ? SenMLParserJSON.parseWithV(chestAccelerationXString)
            : null,
        chestAccelerationYString != null
            ? SenMLParserJSON.parseWithV(chestAccelerationYString)
            : null,
        chestAccelerationZString != null
            ? SenMLParserJSON.parseWithV(chestAccelerationZString)
            : null,
        ECGLead1String != null ? SenMLParserJSON.parseWithV(ECGLead1String) : null,
        ECGLead2String != null ? SenMLParserJSON.parseWithV(ECGLead2String) : null,
        ankleAccelerationXString != null
            ? SenMLParserJSON.parseWithV(ankleAccelerationXString)
            : null,
        ankleAccelerationYString != null
            ? SenMLParserJSON.parseWithV(ankleAccelerationYString)
            : null,
        ankleAccelerationZString != null
            ? SenMLParserJSON.parseWithV(ankleAccelerationZString)
            : null,
        ankleGyroXString != null ? SenMLParserJSON.parseWithV(ankleGyroXString) : null,
        ankleGyroYString != null ? SenMLParserJSON.parseWithV(ankleGyroYString) : null,
        ankleGyroZString != null ? SenMLParserJSON.parseWithV(ankleGyroZString) : null,
        ankleMagnetometerXString != null
            ? SenMLParserJSON.parseWithV(ankleMagnetometerXString)
            : null,
        ankleMagnetometerYString != null
            ? SenMLParserJSON.parseWithV(ankleMagnetometerYString)
            : null,
        ankleMagnetometerZString != null
            ? SenMLParserJSON.parseWithV(ankleMagnetometerZString)
            : null,
        armAccelerationXString != null ? SenMLParserJSON.parseWithV(armAccelerationXString) : null,
        armAccelerationYString != null ? SenMLParserJSON.parseWithV(armAccelerationYString) : null,
        armAccelerationZString != null ? SenMLParserJSON.parseWithV(armAccelerationZString) : null,
        armGyroXString != null ? SenMLParserJSON.parseWithV(armGyroXString) : null,
        armGyroYString != null ? SenMLParserJSON.parseWithV(armGyroYString) : null,
        armGyroZString != null ? SenMLParserJSON.parseWithV(armGyroZString) : null,
        armMagnetometerXString != null ? SenMLParserJSON.parseWithV(armMagnetometerXString) : null,
        armMagnetometerYString != null ? SenMLParserJSON.parseWithV(armMagnetometerYString) : null,
        armMagnetometerZString != null ? SenMLParserJSON.parseWithV(armMagnetometerZString) : null,
        labelString != null ? SenMLParserJSON.parseWithV(labelString) : null);
  }
}
