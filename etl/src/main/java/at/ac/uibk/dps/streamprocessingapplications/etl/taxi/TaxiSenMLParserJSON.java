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
                  if (finalBaseName != null && !jsonObject.has("bn")) {
                    jsonObject.put("bn", finalBaseName);
                  }
                  return jsonObject;
                })
            .collect(
                Collectors.toMap(jsonObject -> jsonObject.getString("n"), JSONObject::toString));

    return buildTaxiRideFromMap(recordNameToRecordString);
  }

  private static TaxiRide buildTaxiRideFromMap(Map<String, String> recordNameToRecordString) {

    String taxiIdentifierString = recordNameToRecordString.get("taxi_identifier");
    String hackLicenseString = recordNameToRecordString.get("hack_license");
    String pickupDatetimeString = recordNameToRecordString.get("pickup_datetime");
    String tripTimeInSecsString = recordNameToRecordString.get("trip_time_in_secs");
    String tripDistanceString = recordNameToRecordString.get("trip_distance");
    String pickupLongitudeString = recordNameToRecordString.get("pickup_longitude");
    String pickupLatitudeString = recordNameToRecordString.get("pickup_latitude");
    String dropoffLongitudeString = recordNameToRecordString.get("dropoff_longitude");
    String dropoffLatitudeString = recordNameToRecordString.get("dropoff_latitude");
    String paymentTypeString = recordNameToRecordString.get("payment_type");
    String fareAmountString = recordNameToRecordString.get("fare_amount");
    String surchargeString = recordNameToRecordString.get("surcharge");
    String mtaTaxString = recordNameToRecordString.get("mta_tax");
    String tipAmountString = recordNameToRecordString.get("tip_amount");
    String tollsAmountString = recordNameToRecordString.get("tolls_amount");
    String totalAmountString = recordNameToRecordString.get("total_amount");

    return new TaxiRide(
        taxiIdentifierString != null ? SenMLParserJSON.parseWithVS(taxiIdentifierString) : null,
        hackLicenseString != null ? SenMLParserJSON.parseWithVS(hackLicenseString) : null,
        pickupDatetimeString != null ? SenMLParserJSON.parseWithVS(pickupDatetimeString) : null,
        tripTimeInSecsString != null ? SenMLParserJSON.parseWithV(tripTimeInSecsString) : null,
        tripDistanceString != null ? SenMLParserJSON.parseWithV(tripDistanceString) : null,
        pickupLongitudeString != null ? SenMLParserJSON.parseWithV(pickupLongitudeString) : null,
        pickupLatitudeString != null ? SenMLParserJSON.parseWithV(pickupLatitudeString) : null,
        dropoffLongitudeString != null ? SenMLParserJSON.parseWithV(dropoffLongitudeString) : null,
        dropoffLatitudeString != null ? SenMLParserJSON.parseWithV(dropoffLatitudeString) : null,
        paymentTypeString != null ? SenMLParserJSON.parseWithVS(paymentTypeString) : null,
        fareAmountString != null ? SenMLParserJSON.parseWithV(fareAmountString) : null,
        surchargeString != null ? SenMLParserJSON.parseWithV(surchargeString) : null,
        mtaTaxString != null ? SenMLParserJSON.parseWithV(mtaTaxString) : null,
        tipAmountString != null ? SenMLParserJSON.parseWithV(tipAmountString) : null,
        tollsAmountString != null ? SenMLParserJSON.parseWithV(tollsAmountString) : null,
        totalAmountString != null ? SenMLParserJSON.parseWithV(totalAmountString) : null);
  }
}
