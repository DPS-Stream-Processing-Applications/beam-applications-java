package at.ac.uibk.dps.streamporcessingapplications.stats;

import at.ac.uibk.dps.streamprocessingapplications.shared.TaxiSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.BloomFilter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Funnel;
import org.joda.time.Duration;

public class TaxiTestObjects {
  public static List<String> testPacks =
      List.of(
          // spotless:off
            "[" +
                "{\"u\": \"string\", \"n\": \"taxi_identifier\", \"vs\": \"2010000001\"}," +
                "{\"u\": \"string\", \"n\": \"hack_license\", \"vs\": \"2010000001\"}," +
                "{\"u\": \"time\", \"n\": \"pickup_datetime\", \"vs\": \"2010-01-01 00:00:00\"}," +
                "{\"u\": \"s\", \"n\": \"trip_time_in_secs\", \"v\": \"34.0\"}," +
                "{\"u\": \"m\", \"n\": \"trip_distance\", \"v\": \"14.05\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_longitude\", \"v\": \"-73.948418\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_latitude\", \"v\": \"40.72459\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_longitude\", \"v\": \"-73.92614\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_latitude\", \"v\": \"40.864761\"}," +
                "{\"u\": \"payment_type\", \"n\": \"payment_type\", \"vs\": \"CAS\"}," +
                "{\"u\": \"dollar\", \"n\": \"fare_amount\", \"v\": \"34.1\"}," +
                "{\"u\": \"%\", \"n\": \"surcharge\", \"v\": \"0.5\"}," +
                "{\"u\": \"%\", \"n\": \"mta_tax\", \"v\": \"0.5\"}," +
                "{\"u\": \"dollar\", \"n\": \"tip_amount\", \"v\": \"0\"}," +
                "{\"u\": \"dollar\", \"n\": \"tolls_amount\", \"v\": \"0.0\"}," +
                "{\"u\": \"dollar\", \"n\": \"total_amount\", \"v\": \"35.1\"}" +
            "]",
            "[" +
                "{\"u\": \"string\", \"n\": \"taxi_identifier\", \"vs\": \"2010000002\"}," +
                "{\"u\": \"string\", \"n\": \"hack_license\", \"vs\": \"2010000002\"}," +
                "{\"u\": \"time\", \"n\": \"pickup_datetime\", \"vs\": \"2010-01-01 00:00:00\"}," +
                "{\"u\": \"s\", \"n\": \"trip_time_in_secs\", \"v\": \"33.0\"}," +
                "{\"u\": \"m\", \"n\": \"trip_distance\", \"v\": \"9.65\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_longitude\", \"v\": \"-73.997414\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_latitude\", \"v\": \"40.736156\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_longitude\", \"v\": \"-73.997833\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_latitude\", \"v\": \"40.736168\"}," +
                "{\"u\": \"payment_type\", \"n\": \"payment_type\", \"vs\": \"CAS\"}," +
                "{\"u\": \"dollar\", \"n\": \"fare_amount\", \"v\": \"27.3\"}," +
                "{\"u\": \"%\", \"n\": \"surcharge\", \"v\": \"0.5\"}," +
                "{\"u\": \"%\", \"n\": \"mta_tax\", \"v\": \"0.5\"}," +
                "{\"u\": \"dollar\", \"n\": \"tip_amount\", \"v\": \"0\"}," +
                "{\"u\": \"dollar\", \"n\": \"tolls_amount\", \"v\": \"0.0\"}," +
                "{\"u\": \"dollar\", \"n\": \"total_amount\", \"v\": \"28.3\"}" +
            "]",
            "[" +
                "{\"u\": \"string\", \"n\": \"taxi_identifier\", \"vs\": \"2010000003\"}," +
                "{\"u\": \"string\", \"n\": \"hack_license\", \"vs\": \"2010000003\"}," +
                "{\"u\": \"time\", \"n\": \"pickup_datetime\", \"vs\": \"2010-01-01 00:00:00\"}," +
                "{\"u\": \"s\", \"n\": \"trip_time_in_secs\", \"v\": \"7.0\"}," +
                "{\"u\": \"m\", \"n\": \"trip_distance\", \"v\": \"1.63\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_longitude\", \"v\": \"-73.967171\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_latitude\", \"v\": \"40.764236\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_longitude\", \"v\": \"-73.956299\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_latitude\", \"v\": \"40.781261\"}," +
                "{\"u\": \"payment_type\", \"n\": \"payment_type\", \"vs\": \"CAS\"}," +
                "{\"u\": \"dollar\", \"n\": \"fare_amount\", \"v\": \"6.9\"}," +
                "{\"u\": \"%\", \"n\": \"surcharge\", \"v\": \"0.5\"}," +
                "{\"u\": \"%\", \"n\": \"mta_tax\", \"v\": \"0.5\"}," +
                "{\"u\": \"dollar\", \"n\": \"tip_amount\", \"v\": \"0\"}," +
                "{\"u\": \"dollar\", \"n\": \"tolls_amount\", \"v\": \"0.0\"}," +
                "{\"u\": \"dollar\", \"n\": \"total_amount\", \"v\": \"7.9\"}" +
            "]",
            "[" +
                "{\"u\": \"string\", \"n\": \"taxi_identifier\", \"vs\": \"2010000004\"}," +
                "{\"u\": \"string\", \"n\": \"hack_license\", \"vs\": \"2010000004\"}," +
                "{\"u\": \"time\", \"n\": \"pickup_datetime\", \"vs\": \"2010-01-01 00:00:00\"}," +
                "{\"u\": \"s\", \"n\": \"trip_time_in_secs\", \"v\": \"33.0\"}," +
                "{\"u\": \"m\", \"n\": \"trip_distance\", \"v\": \"26.61\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_longitude\", \"v\": \"-73.789757\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_latitude\", \"v\": \"40.646526\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_longitude\", \"v\": \"-74.136749\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_latitude\", \"v\": \"40.601543\"}," +
                "{\"u\": \"payment_type\", \"n\": \"payment_type\", \"vs\": \"Cre\"}," +
                "{\"u\": \"dollar\", \"n\": \"fare_amount\", \"v\": \"56.1\"}," +
                "{\"u\": \"%\", \"n\": \"surcharge\", \"v\": \"0.5\"}," +
                "{\"u\": \"%\", \"n\": \"mta_tax\", \"v\": \"0.5\"}," +
                "{\"u\": \"dollar\", \"n\": \"tip_amount\", \"v\": \"10\"}," +
                "{\"u\": \"dollar\", \"n\": \"tolls_amount\", \"v\": \"9.14\"}," +
                "{\"u\": \"dollar\", \"n\": \"total_amount\", \"v\": \"76.24\"}" +
            "]",
            "[" +
                "{\"u\": \"string\", \"n\": \"taxi_identifier\", \"vs\": \"2010000005\"}," +
                "{\"u\": \"string\", \"n\": \"hack_license\", \"vs\": \"2010000005\"}," +
                "{\"u\": \"time\", \"n\": \"pickup_datetime\", \"vs\": \"2010-01-01 00:00:00\"}," +
                "{\"u\": \"s\", \"n\": \"trip_time_in_secs\", \"v\": \"28.0\"}," +
                "{\"u\": \"m\", \"n\": \"trip_distance\", \"v\": \"3.15\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_longitude\", \"v\": \"-73.99955\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_latitude\", \"v\": \"40.731152\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_longitude\", \"v\": \"-73.977448\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_latitude\", \"v\": \"40.763031\"}," +
                "{\"u\": \"payment_type\", \"n\": \"payment_type\", \"vs\": \"CAS\"}," +
                "{\"u\": \"dollar\", \"n\": \"fare_amount\", \"v\": \"14.5\"}," +
                "{\"u\": \"%\", \"n\": \"surcharge\", \"v\": \"0.5\"}," +
                "{\"u\": \"%\", \"n\": \"mta_tax\", \"v\": \"0.5\"}," +
                "{\"u\": \"dollar\", \"n\": \"tip_amount\", \"v\": \"0\"}," +
                "{\"u\": \"dollar\", \"n\": \"tolls_amount\", \"v\": \"0.0\"}," +
                "{\"u\": \"dollar\", \"n\": \"total_amount\", \"v\": \"15.5\"}" +
            "]",
            "[" +
                "{\"u\": \"string\", \"n\": \"taxi_identifier\", \"vs\": \"2010000006\"}," +
                "{\"u\": \"string\", \"n\": \"hack_license\", \"vs\": \"2010000006\"}," +
                "{\"u\": \"time\", \"n\": \"pickup_datetime\", \"vs\": \"2010-01-01 00:00:00\"}," +
                "{\"u\": \"s\", \"n\": \"trip_time_in_secs\", \"v\": \"27.0\"}," +
                "{\"u\": \"m\", \"n\": \"trip_distance\", \"v\": \"11.15\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_longitude\", \"v\": \"-73.993698\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_latitude\", \"v\": \"40.736946\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_longitude\", \"v\": \"-73.861435\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_latitude\", \"v\": \"40.756256\"}," +
                "{\"u\": \"payment_type\", \"n\": \"payment_type\", \"vs\": \"CAS\"}," +
                "{\"u\": \"dollar\", \"n\": \"fare_amount\", \"v\": \"27.7\"}," +
                "{\"u\": \"%\", \"n\": \"surcharge\", \"v\": \"0.5\"}," +
                "{\"u\": \"%\", \"n\": \"mta_tax\", \"v\": \"0.5\"}," +
                "{\"u\": \"dollar\", \"n\": \"tip_amount\", \"v\": \"0\"}," +
                "{\"u\": \"dollar\", \"n\": \"tolls_amount\", \"v\": \"0.0\"}," +
                "{\"u\": \"dollar\", \"n\": \"total_amount\", \"v\": \"28.7\"}" +
            "]",
            "[" +
                "{\"u\": \"string\", \"n\": \"taxi_identifier\", \"vs\": \"2010000007\"}," +
                "{\"u\": \"string\", \"n\": \"hack_license\", \"vs\": \"2010000007\"}," +
                "{\"u\": \"time\", \"n\": \"pickup_datetime\", \"vs\": \"2010-01-01 00:00:00\"}," +
                "{\"u\": \"s\", \"n\": \"trip_time_in_secs\", \"v\": \"18.0\"}," +
                "{\"u\": \"m\", \"n\": \"trip_distance\", \"v\": \"4.3\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_longitude\", \"v\": \"-74.006058\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_latitude\", \"v\": \"40.739925\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_longitude\", \"v\": \"-73.957405\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_latitude\", \"v\": \"40.765686\"}," +
                "{\"u\": \"payment_type\", \"n\": \"payment_type\", \"vs\": \"CAS\"}," +
                "{\"u\": \"dollar\", \"n\": \"fare_amount\", \"v\": \"13.3\"}," +
                "{\"u\": \"%\", \"n\": \"surcharge\", \"v\": \"0.5\"}," +
                "{\"u\": \"%\", \"n\": \"mta_tax\", \"v\": \"0.5\"}," +
                "{\"u\": \"dollar\", \"n\": \"tip_amount\", \"v\": \"0\"}," +
                "{\"u\": \"dollar\", \"n\": \"tolls_amount\", \"v\": \"0.0\"}," +
                "{\"u\": \"dollar\", \"n\": \"total_amount\", \"v\": \"14.3\"}" +
            "]",
            "[" +
                "{\"u\": \"string\", \"n\": \"taxi_identifier\", \"vs\": \"2010000008\"}," +
                "{\"u\": \"string\", \"n\": \"hack_license\", \"vs\": \"2010000008\"}," +
                "{\"u\": \"time\", \"n\": \"pickup_datetime\", \"vs\": \"2010-01-01 00:00:00\"}," +
                "{\"u\": \"s\", \"n\": \"trip_time_in_secs\", \"v\": \"27.0\"}," +
                "{\"u\": \"m\", \"n\": \"trip_distance\", \"v\": \"9.83\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_longitude\", \"v\": \"-73.874245\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_latitude\", \"v\": \"40.773739\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_longitude\", \"v\": \"-74.0028\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_latitude\", \"v\": \"40.760498\"}," +
                "{\"u\": \"payment_type\", \"n\": \"payment_type\", \"vs\": \"Cre\"}," +
                "{\"u\": \"dollar\", \"n\": \"fare_amount\", \"v\": \"25.7\"}," +
                "{\"u\": \"%\", \"n\": \"surcharge\", \"v\": \"0.5\"}," +
                "{\"u\": \"%\", \"n\": \"mta_tax\", \"v\": \"0.5\"}," +
                "{\"u\": \"dollar\", \"n\": \"tip_amount\", \"v\": \"0\"}," +
                "{\"u\": \"dollar\", \"n\": \"tolls_amount\", \"v\": \"4.57\"}," +
                "{\"u\": \"dollar\", \"n\": \"total_amount\", \"v\": \"31.27\"}" +
            "]",
            "[" +
                "{\"u\": \"string\", \"n\": \"taxi_identifier\", \"vs\": \"2010000009\"}," +
                "{\"u\": \"string\", \"n\": \"hack_license\", \"vs\": \"2010000009\"}," +
                "{\"u\": \"time\", \"n\": \"pickup_datetime\", \"vs\": \"2010-01-01 00:00:00\"}," +
                "{\"u\": \"s\", \"n\": \"trip_time_in_secs\", \"v\": \"18.22\"}," +
                "{\"u\": \"m\", \"n\": \"trip_distance\", \"v\": \"3.4\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_longitude\", \"v\": \"-74.004868\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_latitude\", \"v\": \"40.751656\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_longitude\", \"v\": \"-73.988342\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_latitude\", \"v\": \"40.718399\"}," +
                "{\"u\": \"payment_type\", \"n\": \"payment_type\", \"vs\": \"Cas\"}," +
                "{\"u\": \"dollar\", \"n\": \"fare_amount\", \"v\": \"12.5\"}," +
                "{\"u\": \"%\", \"n\": \"surcharge\", \"v\": \"0.5\"}," +
                "{\"u\": \"%\", \"n\": \"mta_tax\", \"v\": \"0.5\"}," +
                "{\"u\": \"dollar\", \"n\": \"tip_amount\", \"v\": \"0\"}," +
                "{\"u\": \"dollar\", \"n\": \"tolls_amount\", \"v\": \"0.0\"}," +
                "{\"u\": \"dollar\", \"n\": \"total_amount\", \"v\": \"13.5\"}" +
            "]",
            "[" +
                "{\"u\": \"string\", \"n\": \"taxi_identifier\", \"vs\": \"2010000010\"}," +
                "{\"u\": \"string\", \"n\": \"hack_license\", \"vs\": \"2010000010\"}," +
                "{\"u\": \"time\", \"n\": \"pickup_datetime\", \"vs\": \"2010-01-01 00:00:02\"}," +
                "{\"u\": \"s\", \"n\": \"trip_time_in_secs\", \"v\": \"36.42\"}," +
                "{\"u\": \"m\", \"n\": \"trip_distance\", \"v\": \"12.4\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_longitude\", \"v\": \"-73.95546\"}," +
                "{\"u\": \"deg\", \"n\": \"pickup_latitude\", \"v\": \"40.787731\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_longitude\", \"v\": \"-73.961739\"}," +
                "{\"u\": \"deg\", \"n\": \"dropoff_latitude\", \"v\": \"40.666935\"}," +
                "{\"u\": \"payment_type\", \"n\": \"payment_type\", \"vs\": \"Cas\"}," +
                "{\"u\": \"dollar\", \"n\": \"fare_amount\", \"v\": \"31.7\"}," +
                "{\"u\": \"%\", \"n\": \"surcharge\", \"v\": \"0.5\"}," +
                "{\"u\": \"%\", \"n\": \"mta_tax\", \"v\": \"0.5\"}," +
                "{\"u\": \"dollar\", \"n\": \"tip_amount\", \"v\": \"0\"}," +
                "{\"u\": \"dollar\", \"n\": \"tolls_amount\", \"v\": \"0.0\"}," +
                "{\"u\": \"dollar\", \"n\": \"total_amount\", \"v\": \"32.7\"}" +
            "]"
    );
    // spotless:on

  public static TestStream<String> buildSimpleTestStream() {
    TestStream.Builder<String> streamBuilder = TestStream.create(StringUtf8Coder.of());
    for (String pack : TaxiTestObjects.testPacks) {
      streamBuilder.addElements(pack).advanceProcessingTime(Duration.standardMinutes(5));
    }
    return streamBuilder.advanceWatermarkToInfinity();
  }

  /** This bloom filter will reject 2 out of the 10 elements in the `testPacks` list. */
  public static BloomFilter<TaxiRide> buildTestBloomFilter() {
    Funnel<TaxiRide> taxiToIdStringFunnel =
        (taxiRide, sink) ->
            sink.putString(taxiRide.getTaxiIdentifier().get(), StandardCharsets.UTF_8);
    BloomFilter<TaxiRide> bloomFilterTaxiRide =
        BloomFilter.create(taxiToIdStringFunnel, TaxiTestObjects.testPacks.size() - 2);
    for (int i = 0; i < TaxiTestObjects.testPacks.size(); i++) {
      // NOTE: Skip two elements
      if (i == 3 || i == 6) {
        continue;
      }
      bloomFilterTaxiRide.put(TaxiSenMLParserJSON.parseSenMLPack(TaxiTestObjects.testPacks.get(i)));
    }
    return bloomFilterTaxiRide;
  }
}
