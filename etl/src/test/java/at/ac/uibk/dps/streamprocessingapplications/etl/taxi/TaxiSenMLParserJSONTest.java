package at.ac.uibk.dps.streamprocessingapplications.etl.taxi;

import static org.junit.jupiter.api.Assertions.*;

import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.model.TaxiRide;
import org.junit.jupiter.api.Test;

class TaxiSenMLParserJSONTest {

  @Test
  void parseSenMLPack() {
    String test =
        "["
            + "{\"u\":\"string\",\"n\":\"taxi_identifier\",\"vs\":\"149298F6D390FA640E80B41ED31199C5\"},"
            + "{\"n\":\"hack_license\",\"u\":\"string\",\"vs\":\"08F944E76118632BE09B9D4B04C7012A\"},"
            + "{\"u\":\"time\",\"n\":\"pickup_datetime\",\"vs\":\"2013-01-13 23:36:00\"},"
            + "{\"n\":\"trip_time_in_secs\",\"u\":\"second\",\"v\":1440},"
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
            + "{\"n\":\"total_amount\",\"u\":\"dollar\",\"v\":30.00}"
            + "]";
    TaxiRide ride = TaxiSenMLParserJSON.parseSenMLPack(test);
    System.out.println(ride);
  }
}
