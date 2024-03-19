package at.ac.uibk.dps.streamprocessingapplications.utils;

import at.ac.uibk.dps.streamprocessingapplications.TrainJob;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.Measurement;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.SensorData;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.Taxi_Trip;
import com.google.gson.Gson;
import com.opencsv.CSVReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

public class TaxiDataGenerator {
    private static long rowToParse = 0;
    private String dataSetPath;

    private boolean isCsvFile;

    public TaxiDataGenerator(String dataSetPath, boolean isCsvFile) {
        this.dataSetPath = dataSetPath;
        this.isCsvFile = isCsvFile;
    }

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

    public Taxi_Trip getNextDataEntry() {
        String csvFile = dataSetPath;
        long totalNumberLines = TrainJob.countLines(csvFile);
        rowToParse = rowToParse % totalNumberLines;

        Taxi_Trip taxiTrip = new Taxi_Trip();
        try {
            if (isCsvFile) {
                Gson gson = new Gson();
                CSVReader reader = new CSVReader(new FileReader(csvFile), '|');
                String[] row;
                int currentRow = 0;
                while ((row = reader.readNext()) != null && currentRow < rowToParse) {
                    currentRow++;
                }

                long counter = 0;
                if (row != null) {
                    String json =
                            Arrays.toString(row).substring(1, Arrays.toString(row).length() - 1);
                    json = json.replaceFirst("\\{", "");
                    json = "{ts:" + json;

                    Measurement measurement = gson.fromJson(json, Measurement.class);

                    for (SensorData entry : measurement.getSensorDataList()) {

                        if (Objects.equals(entry.getN(), "trip_time_in_secs")) {
                            taxiTrip.setTrip_time_in_secs(entry.getV());
                            counter++;
                        }
                        if (Objects.equals(entry.getN(), "trip_distance")) {
                            taxiTrip.setTrip_distance(entry.getV());
                            counter++;
                        }
                        if (Objects.equals(entry.getN(), "fare_amount")) {
                            taxiTrip.setFare_amount(entry.getV());
                            counter++;
                        }
                    }
                }
                if (counter != 3) {
                    throw new RuntimeException("Counter is not correct!");
                }
            } else {
                if (rowToParse == 0) {
                    rowToParse = 1;
                }
                CSVReader reader = new CSVReader(new FileReader(csvFile), ',');
                String[] row;
                int currentRow = 0;
                while ((row = reader.readNext()) != null && currentRow < rowToParse) {
                    currentRow++;
                }
                if (row != null) {
                    taxiTrip.setTrip_time_in_secs(row[4]);
                    taxiTrip.setTrip_distance(row[5]);
                    taxiTrip.setFare_amount(row[11]);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error when reading row " + e);
        }
        rowToParse++;
        return taxiTrip;
    }
}
