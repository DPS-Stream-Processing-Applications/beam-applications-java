package at.ac.uibk.dps.streamprocessingapplications.etl;

import static org.junit.jupiter.api.Assertions.*;

import at.ac.uibk.dps.streamprocessingapplications.etl.model.SenMLRecordDouble;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenMLParserTest {

  private SenMLRecordDouble record1;
  private JSONObject jsonObject1;
  private String senMLString1;

  private SenMLRecordDouble record2;
  private JSONObject jsonObject2;
  private String senMLString2;

  @BeforeEach
  void setUp() {
    record1 =
        new SenMLRecordDouble(
            "149298F6D390FA640E80B41ED31199C5",
            "trip_time_in_secs",
            "s",
            1440.0,
            Instant.ofEpochSecond(1358116560));
    jsonObject1 =
        new JSONObject(
            Map.of(
                "bn",
                record1.getBaseName(),
                "n",
                record1.getName(),
                "u",
                record1.getUnit(),
                "v",
                record1.getValue(),
                "t",
                record1.getTime().getEpochSecond()));
    senMLString1 = jsonObject1.toString();

   record2 =
        new SenMLRecordDouble(
            "149298F6D390FA640E80B41ED31199C5",
            "pickup_datetime",
            "s",
            1358116560.0,
            Instant.ofEpochSecond(1358116560));
    jsonObject2 =
        new JSONObject(
            Map.of(
                "bn",
                record2.getBaseName(),
                "n",
                record2.getName(),
                "u",
                record2.getUnit(),
                "v",
                record2.getValue(),
                "t",
                record2.getTime().getEpochSecond()));
    senMLString2 = jsonObject2.toString();
  }

  @Test
  void parseJsonStringWithV() {
    assertEquals(record1, SenMLParser.parseJsonStringWithV(senMLString1));
  }

  @Test
  void parseSenMLPack_empty() {
    JSONArray pack = new JSONArray(List.of());
    String packSenMLString = pack.toString();

    assertEquals(Set.of(), SenMLParser.parseSenMLPack(packSenMLString, SenMLParser::parseJsonStringWithV));
  }

  @Test
  void parseSenMLPack_oneElement() {
    JSONArray pack = new JSONArray(List.of(jsonObject1));
    String packSenMLString = pack.toString();

    assertEquals(Set.of(record1), SenMLParser.parseSenMLPack(packSenMLString, SenMLParser::parseJsonStringWithV));
  }

  @Test
  void parseSenMLPack_twoElements() {
    JSONArray pack = new JSONArray(List.of(jsonObject1, jsonObject2));
    String packSenMLString = pack.toString();

    assertEquals(Set.of(record1, record2), SenMLParser.parseSenMLPack(packSenMLString, SenMLParser::parseJsonStringWithV));
  }

  @Test
  void parseSenMLPack_twoElementsWithOneMissingBasename_InfersBaseName() {
    JSONObject jsonObject2NoBaseName = new JSONObject(jsonObject2.toString());
    jsonObject2NoBaseName.remove("bn");
    JSONArray pack = new JSONArray(List.of(jsonObject1, jsonObject2NoBaseName));
    String packSenMLString = pack.toString();

    assertEquals(Set.of(record1, record2), SenMLParser.parseSenMLPack(packSenMLString, SenMLParser::parseJsonStringWithV));
  }
}
