package at.ac.uibk.dps.streamprocessingapplications.shared;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import org.junit.jupiter.api.Test;

class TaxiSenMLParserJSONTest {

  // TODO: Add proper tests
  @Test
  void parseSenMLPack_allExpectedRecords() {
    String test =
        "[{\"u\":\"string\",\"n\":\"taxi_identifier\",\"vs\":\"149298F6D390FA640E80B41ED31199C5\"},"
            + "{\"n\":\"hack_license\",\"u\":\"string\",\"vs\":\"08F944E76118632BE09B9D4B04C7012A\"},"
            + "{\"u\":\"time\",\"n\":\"pickup_datetime\",\"vs\":\"2013-01-13"
            + " 23:36:00\"},{\"n\":\"trip_time_in_secs\",\"u\":\"second\",\"v\":1440},"
            + "{\"n\":\"trip_distance\",\"u\":\"meter\",\"v\":9.08},"
            + "{\"n\":\"pickup_longitude\",\"u\":\"lon\",\"vs\":\"-73.982071\"},"
            + "{\"n\":\"pickup_latitude\",\"u\":\"lat\",\"vs\":\"40.769081\"},"
            + "{\"n\":\"dropoff_longitude\",\"u\":\"lon\",\"vs\":\"-73.915878\"},"
            + "{\"n\":\"dropoff_latitude\",\"u\":\"lat\",\"vs\":\"40.868458\"},"
            + "{\"n\":\"payment_type\",\"u\":\"string\",\"vs\":\"CSH\"},"
            + "{\"n\":\"fare_amount\",\"u\":\"dollar\",\"v\":29.00},"
            + "{\"n\":\"surcharge\",\"u\":\"percentage\",\"v\":0.50},"
            + "{\"n\":\"mta_tax\",\"u\":\"percentage\",\"v\":0.50},"
            + "{\"n\":\"tip_amount\",\"u\":\"dollar\",\"v\":0.00},"
            + "{\"n\":\"tolls_amount\",\"u\":\"dollar\",\"v\":0.00},"
            + "{\"n\":\"total_amount\",\"u\":\"dollar\",\"v\":30.00}]";
    TaxiRide ride = TaxiSenMLParserJSON.parseSenMLPack(test);
    System.out.println(ride);
  }

  @Test
  void parseSenMLPack_allExpectedRecords_2() {
    String test =
        "[{\"u\":  \"string\",\"n\":  \"taxi_identifier\",\"vs\":  \"2013007491\"},{\"u\": "
            + " \"string\",\"n\":  \"hack_license\",\"vs\":  \"2013007487\"},{\"u\":  \"time\", "
            + " \"n\":  \"pickup_datetime\",\"vs\":  \"2013-01-14  00:00:00\"},{\"u\": "
            + " \"s\",\"n\":  \"trip_time_in_secs\",\"v\":  \"1080\"},{\"u\":  \"m\",\"n\": "
            + " \"trip_distance\",\"v\":  \"9.54\"},{\"u\":  \"deg\",\"n\": "
            + " \"pickup_longitude\",\"v\":  \"-73.862625\"},{\"u\":  \"deg\",\"n\": "
            + " \"pickup_latitude\",\"v\":  \"40.768902\"},{\"u\":  \"deg\",\"n\": "
            + " \"dropoff_longitude\",\"v\":  \"-73.987274\"},{\"u\":  \"deg\",\"n\": "
            + " \"dropoff_latitude\",\"v\":  \"40.7439\"},{\"u\":  \"payment_type\",\"n\": "
            + " \"payment_type\",\"vs\":  \"CRD\"},{\"u\":  \"dollar\",\"n\": "
            + " \"fare_amount\",\"v\":  \"7.5\"},{\"u\":  \"%\",\"n\":  \"surcharge\",\"v\": "
            + " \"0.5\"},{\"u\":  \"%\",\"n\":  \"mta_tax\",\"v\":  \"0.5\"},{\"u\": "
            + " \"dollar\",\"n\":  \"tip_amount\",\"v\":  \"1.6\"},{\"u\":  \"dollar\",\"n\": "
            + " \"tolls_amount\",\"v\":  \"0.0\"},{\"u\":  \"dollar\",\"n\": "
            + " \"total_amount\",\"v\":  \"10.1\"}]";
    TaxiRide ride = TaxiSenMLParserJSON.parseSenMLPack(test);
    System.out.println(ride);
  }
}
