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
        .addElements(
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 25.7, \"t\": 1645467500}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 51, \"t\": 1645467500}")
        .advanceProcessingTime(Duration.standardMinutes(5))
        .addElements(
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 26.2, \"t\": 1645467800}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 52, \"t\": 1645467800}")
        .advanceProcessingTime(Duration.standardMinutes(5))
        .addElements(
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 26.8, \"t\": 1645468100}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 53, \"t\": 1645468100}")
        .advanceProcessingTime(Duration.standardMinutes(5))
        .addElements(
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 27.1, \"t\": 1645468400}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 54, \"t\": 1645468400}")
        .advanceProcessingTime(Duration.standardMinutes(5))
        .addElements(
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 27.5, \"t\": 1645468700}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 55, \"t\": 1645468700}")
        .advanceProcessingTime(Duration.standardMinutes(5))
        .addElements(
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 27.9, \"t\": 1645469000}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 56, \"t\": 1645469000}")
        .advanceProcessingTime(Duration.standardMinutes(5))
        .addElements(
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 28.3, \"t\": 1645469300}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 57, \"t\": 1645469300}")
        .advanceProcessingTime(Duration.standardMinutes(5))
        .addElements(
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 28.7, \"t\": 1645469600}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 58, \"t\": 1645469600}")
        .advanceProcessingTime(Duration.standardMinutes(5))
        .addElements(
            "{\"bn\": \"temperature_sensor\", \"n\": \"temperature\", \"u\": \"Cel\","
                + " \"v\": 29.2, \"t\": 1645469900}",
            "{\"bn\": \"humidity_sensor\", \"n\": \"humidity\", \"u\": \"%RH\", \"v\":"
                + " 59, \"t\": 1645469900}")
        .advanceProcessingTime(Duration.standardMinutes(5))
        .advanceWatermarkToInfinity();
  }
}
