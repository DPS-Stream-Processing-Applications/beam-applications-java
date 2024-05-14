package at.ac.uibk.dps.streamprocessingapplications.shared;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.FitnessMeasurements;
import org.junit.jupiter.api.Test;

public class FitSenMLParserJSONTest {
    @Test
    void parseSenMLPack_allExpectedRecords() {
        String test ="[{\"u\":\"string\",\"n\":\"subjectId\",\"sv\":\"subject8\"},{\"v\":\"-9.3296\",\"u\":\"m/s2\",\"n\":\"acc_chest_x\"},{\"v\":\"-0.6898\",\"u\":\"m/s2\",\"n\":\"acc_chest_y\"},{\"v\":\"2.0748\",\"u\":\"m/s2\",\"n\":\"acc_chest_z\"},{\"v\":\"0.062794\",\"u\":\"mV\",\"n\":\"ecg_lead_1\"},{\"v\":\"0.037677\",\"u\":\"mV\",\"n\":\"ecg_lead_2\"},{\"v\":\"-0.41798\",\"u\":\"m/s2\",\"n\":\"acc_ankle_x\"},{\"v\":\"-9.7781\",\"u\":\"m/s2\",\"n\":\"acc_ankle_y\"},{\"v\":\"-1.5764\",\"u\":\"m/s2\",\"n\":\"acc_ankle_z\"},{\"v\":\"-0.14657\",\"u\":\"deg/s\",\"n\":\"gyro_ankle_x\"},{\"v\":\"-0.16886\",\"u\":\"deg/s\",\"n\":\"gyro_ankle_y\"},{\"v\":\"0.76228\",\"u\":\"deg/s\",\"n\":\"gyro_ankle_z\"},{\"v\":\"-0.54617\",\"u\":\"tesla\",\"n\":\"magnetometer_ankle_x\"},{\"v\":\"0.0054617\",\"u\":\"tesla\",\"n\":\"magnetometer_ankle_y\"},{\"v\":\"0.15517\",\"u\":\"tesla\",\"n\":\"magnetometer_ankle_z\"},{\"v\":\"-1.9841\",\"u\":\"m/s2\",\"n\":\"acc_arm_x\"},{\"v\":\"-9.6963\",\"u\":\"m/s2\",\"n\":\"acc_arm_y\"},{\"v\":\"-0.63065\",\"u\":\"m/s2\",\"n\":\"acc_arm_z\"},{\"v\":\"-0.6902\",\"u\":\"deg/s\",\"n\":\"gyro_arm_x\"},{\"v\":\"-0.2731\",\"u\":\"deg/s\",\"n\":\"gyro_arm_y\"},{\"v\":\"-0.44181\",\"u\":\"deg/s\",\"n\":\"gyro_arm_z\"},{\"v\":\"0.0035208\",\"u\":\"tesla\",\"n\":\"magnetometer_arm_x\"},{\"v\":\"0.35208\",\"u\":\"tesla\",\"n\":\"magnetometer_arm_y\"},{\"v\":\"-1.0865\",\"u\":\"tesla\",\"n\":\"magnetometer_arm_z\"},{\"v\":\"0\",\"u\":\"number \",\"n\":\"label\"}],\"bt\":\"1417890600020\"}";
        FitnessMeasurements measurements = FitSenMLParserJSON.parseSenMLPack(test);
        // NOTE: Check if toString also works for repeated parsing
        FitnessMeasurements newMeasurements = FitSenMLParserJSON.parseSenMLPack(measurements.toString());
        System.out.println(measurements);
        System.out.println(newMeasurements);
    }
}

