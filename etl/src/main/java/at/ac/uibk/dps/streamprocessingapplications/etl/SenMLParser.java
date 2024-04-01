package at.ac.uibk.dps.streamprocessingapplications.etl;

import at.ac.uibk.dps.streamprocessingapplications.etl.model.AbstractSenMLRecord;
import at.ac.uibk.dps.streamprocessingapplications.etl.model.SenMLRecordDouble;
import at.ac.uibk.dps.streamprocessingapplications.etl.model.SenMLRecordString;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import org.json.JSONArray;
import org.json.JSONObject;

public class SenMLParser {
  public static SenMLRecordDouble parseJsonStringWithV(String senMLJson) {
    JSONObject record = new JSONObject(senMLJson);

    String baseName = record.has("bn") ? record.optString("bn") : null;
    String name = record.has("n") ? record.optString("n") : null;
    String unit = record.has("u") ? record.optString("u") : null;
    Double value = record.has("v") ? Double.valueOf(record.optString("v")) : null;
    Instant time =
        record.has("t") ? Instant.ofEpochSecond(Long.parseLong(record.optString("t"))) : null;

    return new SenMLRecordDouble(baseName, name, unit, value, time);
  }

  public static SenMLRecordString parseJsonStringWithVS(String senMLJson) {
    JSONObject record = new JSONObject(senMLJson);

    String baseName = record.has("bn") ? record.optString("bn") : null;
    String name = record.has("n") ? record.optString("n") : null;
    String unit = record.has("u") ? record.optString("u") : null;
    String value = record.has("vs") ? record.optString("vs") : null;
    Instant time =
        record.has("t") ? Instant.ofEpochSecond(Long.parseLong(record.optString("t"))) : null;

    return new SenMLRecordString(baseName, name, unit, value, time);
  }

  public static <T extends AbstractSenMLRecord<?>> Set<T> parseSenMLPack(
      String senMLJsonArray, Function<String, T> recordParser) {
    JSONArray recordPack = new JSONArray(senMLJsonArray);
    Set<T> records = new HashSet<>();

    if (recordPack.isEmpty()) {
      return records;
    }

    T baseRecord = recordParser.apply(recordPack.getJSONObject(0).toString());
    records.add(baseRecord);
    recordPack.remove(0);
    recordPack
        .iterator()
        .forEachRemaining(
            json -> {
              T record = recordParser.apply(json.toString());
              if (record.getBaseName() == null) {
                record.setBaseName(baseRecord.getBaseName());
              }
              records.add(record);
            });
    return records;
  }
}
