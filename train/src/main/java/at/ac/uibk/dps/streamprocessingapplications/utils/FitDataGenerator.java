package at.ac.uibk.dps.streamprocessingapplications.utils;

import at.ac.uibk.dps.streamprocessingapplications.TrainJob;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.FIT_data;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.Measurement;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.SensorData;
import com.google.gson.Gson;
import com.opencsv.CSVReader;

import java.io.FileReader;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

public class FitDataGenerator {
    private static long rowToParse = 0;

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

    public static FIT_data getNextDataEntry() {
        // FIXME!
        String csvFile = "./train/src/main/resources/datasets/FIT_sample_data_senml.csv";
        long totalNumberLines = TrainJob.countLines(csvFile);
        rowToParse = rowToParse % totalNumberLines;

        FIT_data fitData = new FIT_data();
        try {
            Gson gson = new Gson();
            CSVReader reader = new CSVReader(new FileReader(csvFile), '|');
            String[] row;
            int currentRow = 0;
            while ((row = reader.readNext()) != null && currentRow < rowToParse) {
                currentRow++;
            }

            if (row != null) {
                String json = Arrays.toString(row).substring(1, Arrays.toString(row).length() - 1);
                json = json.replaceFirst("\\{", "");
                json = "{ts:" + json;

                Measurement measurement = gson.fromJson(json, Measurement.class);

                for (SensorData entry : measurement.getSensorDataList()) {

                    if (Objects.equals(entry.getN(), "acc_ankle_x")) {
                        fitData.setAcc_ankle_x(entry.getV());
                    }
                    if (Objects.equals(entry.getN(), "acc_ankle_y")) {
                        fitData.setAcc_ankle_y(entry.getV());
                    }
                    if (Objects.equals(entry.getN(), "acc_ankle_z")) {
                        fitData.setAcc_ankle_z(entry.getV());
                    }
                    if (Objects.equals(entry.getN(), "acc_arm_x")) {
                        fitData.setAcc_arm_x(entry.getV());
                    }
                    if (Objects.equals(entry.getN(), "acc_arm_y")) {
                        fitData.setAcc_arm_y(entry.getV());
                    }
                    if (Objects.equals(entry.getN(), "acc_arm_z")) {
                        fitData.setAcc_arm_z(entry.getV());
                    }
                    if (Objects.equals(entry.getN(), "acc_chest_x")) {
                        fitData.setAcc_chest_x(entry.getV());
                    }
                    if (Objects.equals(entry.getN(), "acc_chest_y")) {
                        fitData.setAcc_chest_y(entry.getV());
                    }
                    if (Objects.equals(entry.getN(), "acc_chest_z")) {
                        fitData.setAcc_chest_z(entry.getV());
                    }
                    if (Objects.equals(entry.getN(), "ecg_lead_1")) {
                        fitData.setEcg_lead_1(entry.getV());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error when reading row " + e);
        }
        rowToParse++;
        return fitData;
    }
}
