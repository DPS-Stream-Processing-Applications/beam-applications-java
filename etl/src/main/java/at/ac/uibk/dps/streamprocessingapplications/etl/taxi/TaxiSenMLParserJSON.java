package at.ac.uibk.dps.streamprocessingapplications.etl.taxi;

import at.ac.uibk.dps.streamprocessingapplications.etl.SenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.model.TaxiRide;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class TaxiSenMLParserJSON {
  /**
   * Parses a JSON Array for the TAXI dataset. (see Example) Usually a SenML pack stores multiple
   * values for the same sensor Due to the incorrect use of a SenML pack from the developers of
   * riot-bench all values need to be stored as a `SenMLRecordString`.
   *
   * @param senMLJsonArray The JSON array string to parse.
   * @return The set of all the contained SenML records.<br>
   *     <br>
   *     Example: <br>
   *     <pre>{@code
   * [
   *   {"u":"string","n":"taxi_identifier","vs":"149298F6D390FA640E80B41ED31199C5"},
   *   {"n":"hack_license","u":"string","vs":"08F944E76118632BE09B9D4B04C7012A"},
   *   {"u":"time","n":"pickup_datetime","vs":"2013-01-13 23:36:00"},
   *   {"v":"1440","u":"second","n":"trip_time_in_secs"},
   *   {"v":"9.08","u":"meter","n":"trip_distance"},
   *   {"u":"lon","n":"pickup_longitude","vs":"-73.982071"},
   *   {"u":"lat","n":"pickup_latitude","vs":"40.769081"},
   *   {"u":"lon","n":"dropoff_longitude","vs":"-73.915878"},
   *   {"u":"lat","n":"dropoff_latitude","vs":"40.868458"},
   *   {"u":"string","n":"payment_type","vs":"CSH"},
   *   {"v":"29.00","u":"dollar","n":"fare_amount"},
   *   {"v":"0.50","u":"percentage","n":"surcharge"},
   *   {"v":"0.50","u":"percentage","n":"mta_tax"},
   *   {"v":"0.00","u":"dollar","n":"tip_amount"},
   *   {"v":"0.00","u":"dollar","n":"tolls_amount"},
   *   {"v":"30.00","u":"dollar","n":"total_amount"}
   * ]
   * }</pre>
   */
  public static TaxiRide parseSenMLPack(String senMLJsonArray) {
    JSONArray recordPack = new JSONArray(senMLJsonArray);
    if (recordPack.isEmpty()) {
      return new TaxiRide();
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
                  // NOTE: Add basename to each record if not already present
                  if (finalBaseName != null && !jsonObject.has("bn")) {
                    jsonObject.put("bn", finalBaseName);
                  }
                  return jsonObject;
                })
            .collect(
                Collectors.toMap(jsonObject -> jsonObject.getString("n"), JSONObject::toString));

    // TODO: Allow for Values to not exist and pass them as null. Right now `parseWith()` throws
    // NullPointerException.
    return new TaxiRide(
        SenMLParserJSON.parseWithVS(recordNameToRecordString.get("taxi_identifier")),
        SenMLParserJSON.parseWithVS(recordNameToRecordString.get("hack_license")),
        SenMLParserJSON.parseWithVS(recordNameToRecordString.get("pickup_datetime")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("trip_time_in_secs")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("trip_distance")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("pickup_longitude")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("pickup_latitude")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("dropoff_longitude")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("dropoff_latitude")),
        SenMLParserJSON.parseWithVS(recordNameToRecordString.get("payment_type")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("fare_amount")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("surcharge")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("mta_tax")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("tip_amount")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("tolls_amount")),
        SenMLParserJSON.parseWithV(recordNameToRecordString.get("total_amount")));
  }
}
