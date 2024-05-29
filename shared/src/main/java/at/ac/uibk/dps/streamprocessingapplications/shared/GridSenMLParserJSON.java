package at.ac.uibk.dps.streamprocessingapplications.shared;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GridSenMLParserJSON {

    public static GridMeasurement parseSenMLPack(String senMLJsonArray) {
        JSONArray recordPack = new JSONArray(senMLJsonArray);
        if (recordPack.isEmpty()) {
            return new GridMeasurement();
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

        return buildGridMeasurementFromMap(recordNameToRecordString);
    }

    private static GridMeasurement buildGridMeasurementFromMap(Map<String, String> recordNameToRecordString) {
        String id = recordNameToRecordString.get("id");
        String measurement = recordNameToRecordString.get("grid_measurement");
        String timestamp = recordNameToRecordString.get("timestamp");

        return new GridMeasurement(
                id != null ? SenMLParserJSON.parseWithV(id) : null,
                measurement != null ? SenMLParserJSON.parseWithV(measurement) : null,
                timestamp != null ? SenMLParserJSON.parseWithV(timestamp) : null
        );
    }
}
