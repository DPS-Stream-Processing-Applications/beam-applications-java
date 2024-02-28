package at.ac.uibk.dps.streamprocessingapplications.utils;

import at.ac.uibk.dps.streamprocessingapplications.entity.FIT_data;
import java.util.Random;

public class FitDataGenerator {

    // Generate random FIT_data object
    public static FIT_data generateRandomFITData() {
        FIT_data fitData = new FIT_data();
        Random random = new Random();

        // Generate random values for each field
        fitData.setSubjectId(String.valueOf(random.nextDouble()));
        fitData.setAcc_ankle_x(String.valueOf(random.nextDouble()));
        fitData.setAcc_ankle_y(String.valueOf(random.nextDouble()));
        fitData.setAcc_ankle_z(String.valueOf(random.nextDouble()));

        fitData.setEcg_lead_1(String.valueOf(random.nextDouble()));

        fitData.setAcc_arm_x(String.valueOf(random.nextDouble()));
        fitData.setAcc_arm_y(String.valueOf(random.nextDouble()));
        fitData.setAcc_arm_z(String.valueOf(random.nextDouble()));

        fitData.setAcc_chest_x(String.valueOf(random.nextDouble()));
        fitData.setAcc_chest_y(String.valueOf(random.nextDouble()));
        fitData.setAcc_chest_z(String.valueOf(random.nextDouble()));

        return fitData;
    }

    private static String generateRandomString() {
        int leftLimit = 97;
        int rightLimit = 122;
        int targetStringLength = 10;
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}
