package at.ac.uibk.dps.streamprocessingapplications.utils;

import at.ac.uibk.dps.streamprocessingapplications.entity.azure.SYS_City;
import java.util.Random;

public class CityDataGenerator {

    public static SYS_City generateRandomCityData() {
        SYS_City sysData = new SYS_City();
        Random random = new Random();

        // Generate random values for each field
        sysData.setHumidity(String.valueOf(random.nextDouble()));
        sysData.setDust(String.valueOf(random.nextDouble()));
        sysData.setLatitude(String.valueOf(random.nextDouble()));
        sysData.setLocation(String.valueOf(random.nextDouble()));
        sysData.setLight(String.valueOf(random.nextDouble()));
        sysData.setTs(String.valueOf(random.nextDouble()));
        sysData.setSource(String.valueOf(random.nextDouble()));
        sysData.setLongitude(String.valueOf(random.nextDouble()));
        sysData.setHumidity(String.valueOf(random.nextDouble()));
        sysData.setAirquality_raw(String.valueOf(random.nextDouble()));

        return sysData;
    }
}
