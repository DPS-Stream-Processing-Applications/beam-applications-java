package at.ac.uibk.dps.streamprocessingapplications.utils;

import at.ac.uibk.dps.streamprocessingapplications.entity.azure.Taxi_Trip;
import java.util.Random;

public class TaxiDataGenerator {

    public static Taxi_Trip generateRandomTaxiData() {
        Taxi_Trip sysData = new Taxi_Trip();
        Random random = new Random();

        sysData.setTaxi_identifier(String.valueOf(random.nextDouble()));
        sysData.setHack_license(String.valueOf(random.nextDouble()));
        sysData.setPickup_datetime(String.valueOf(random.nextDouble()));
        sysData.setDrop_datetime(String.valueOf(random.nextDouble()));
        sysData.setTrip_time_in_secs(String.valueOf(random.nextDouble()));
        sysData.setTrip_distance(String.valueOf(random.nextDouble()));
        sysData.setPickup_longitude(String.valueOf(random.nextDouble()));
        sysData.setPickup_latitude(String.valueOf(random.nextDouble()));
        sysData.setDropoff_longitude(String.valueOf(random.nextDouble()));
        sysData.setDropoff_latitude(String.valueOf(random.nextDouble()));
        sysData.setPayment_type(String.valueOf(random.nextDouble()));
        sysData.setFare_amount(String.valueOf(random.nextDouble()));
        sysData.setSurcharge(String.valueOf(random.nextDouble()));
        sysData.setMta_tax(String.valueOf(random.nextDouble()));
        sysData.setTip_amount(String.valueOf(random.nextDouble()));
        sysData.setTolls_amount(String.valueOf(random.nextDouble()));
        sysData.setTotal_amount(String.valueOf(random.nextDouble()));

        return sysData;
    }
}
