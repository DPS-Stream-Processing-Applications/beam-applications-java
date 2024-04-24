package at.ac.uibk.dps.streamprocessingapplications.etl.transforms;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.joda.time.Duration;

public class OnTimeTestStream {
  public static TestStream<String> create() {
    return TestStream.create(StringUtf8Coder.of())
        .addElements(
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 25.3, \"t\": 1645467200}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 50, \"t\": 1645467200}")
        .advanceProcessingTime(Duration.standardMinutes(5))
        /*{
          "e":
          [
            {"u":"string","n":"source","sv":"ci4lr75sl000802ypo4qrcjda23"},
            {"v":"6.1668213","u":"lon","n":"longitude"},
            {"v":"46.1927629","u":"lat","n":"latitude"},
            {"v":"8","u":"far","n":"temperature"},
            {"v":"53.7","u":"per","n":"humidity"},
            {"v":"0","u":"per","n":"light"},
            {"v":"411.02","u":"per","n":"dust"},
            {"v":"140","u":"per","n":"airquality_raw"}
          ],
          "bt":1422748800000
          }
        */
        .advanceWatermarkToInfinity();
  }
}
