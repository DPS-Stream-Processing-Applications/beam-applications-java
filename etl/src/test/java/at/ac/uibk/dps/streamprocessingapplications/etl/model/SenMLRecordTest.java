package at.ac.uibk.dps.streamprocessingapplications.etl.model;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenMLRecordTest {

  private String baseName;
  private String name;
  private String unit;
  private float value;
  private long time;

  private JSONObject jsonObject;

  private SenMLRecord record;
  private SenMLRecord emptyRecord;

  @BeforeEach
  public void setUp() {
    baseName = "urn:dev:ow:9e2073a01080063:";
    name = "temperature";
    unit = "Cel";
    value = 25.1f;
    time = 1365839838;

    jsonObject = new JSONObject();
    jsonObject.put("bn", baseName);
    jsonObject.put("n", name);
    jsonObject.put("u", unit);
    jsonObject.put("v", value);
    jsonObject.put("t", time);

    record = new SenMLRecord(jsonObject.toString());
    emptyRecord = new SenMLRecord("{}");
  }

  @Test
  void getBaseName_bnContainedInString_returnsBaseNameString() {
    assertEquals(baseName, record.getBaseName());
  }

  @Test
  void getBaseName_bnNotContainedInString_returnsNull() {
    assertNull(emptyRecord.getBaseName());
  }

  @Test
  void getName_nContainedInString_returnsNameString() {
    assertEquals(name, record.getName());
  }

  @Test
  void getName_nNotContainedInString_returnsNull() {
    assertNull(emptyRecord.getName());
  }

  @Test
  void getUnit_uContainedInString_returnsUnitString() {
    assertEquals(unit, record.getUnit());
  }

  @Test
  void getUnit_uNotContainedInString_returnsNull() {
    assertNull(emptyRecord.getUnit());
  }

  @Test
  void getValue_vContainedInString_returnsValueFloat() {
    assertEquals(value, record.getValue());
  }

  @Test
  void getValue_vNotContainedInString_returnsNull() {
    assertNull(emptyRecord.getValue());
  }

  @Test
  void getTime_tContainedInString_returnsTimeAsInstant() {
    assertEquals(Instant.ofEpochSecond(time), record.getTime());
  }

  @Test
  void getTime_tNotContainedInString_returnsNull() {
    assertNull(emptyRecord.getTime());
  }
}
