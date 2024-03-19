package at.ac.uibk.dps.streamprocessingapplications.utils;

import at.ac.uibk.dps.streamprocessingapplications.TrainJob;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.Measurement;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.SYS_City;
import at.ac.uibk.dps.streamprocessingapplications.entity.azure.SensorData;
import com.google.gson.Gson;
import com.opencsv.CSVReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

public class CityDataGenerator {
    private static long rowToParse = 0;

    private String dataSetFileName;

    private boolean isCsvFile;

    public CityDataGenerator(String dataSetFileName, boolean isCsvFile) {
        this.dataSetFileName = dataSetFileName;
        this.isCsvFile = isCsvFile;
    }

    public static SYS_City generateRandomCityData() {
        SYS_City sysData = new SYS_City();
        Random random = new Random();

        // Generate random values for each field
        sysData.setHumidity(String.valueOf(random.nextDouble()));
        sysData.setDust(String.valueOf(random.nextDouble()));
        sysData.setLatitude(String.valueOf(random.nextDouble()));
        sysData.setLight(String.valueOf(random.nextDouble()));
        sysData.setTs(String.valueOf(random.nextDouble()));
        sysData.setSource(String.valueOf(random.nextDouble()));
        sysData.setLongitude(String.valueOf(random.nextDouble()));
        sysData.setHumidity(String.valueOf(random.nextDouble()));
        sysData.setAirquality_raw(String.valueOf(random.nextDouble()));

        return sysData;
    }

    public SYS_City getNextDataEntry() {
        String csvFile = dataSetFileName;
        long totalNumberLines = TrainJob.countLines(csvFile);
        rowToParse = rowToParse % totalNumberLines;
        SYS_City sysCity = new SYS_City();
        try {
            if (isCsvFile) {
                Gson gson = new Gson();
                CSVReader reader = new CSVReader(new FileReader(csvFile), '|');
                String[] row;
                int currentRow = 0;
                while ((row = reader.readNext()) != null && currentRow < rowToParse) {
                    currentRow++;
                }

                if (row != null) {
                    String json =
                            Arrays.toString(row).substring(1, Arrays.toString(row).length() - 1);
                    json = json.replaceFirst("\\{", "");
                    json = "{ts:" + json;

                    // Parse JSON using Gson
                    Measurement measurement = gson.fromJson(json, Measurement.class);
                    sysCity.setTs(measurement.getTs());
                    for (SensorData entry : measurement.getSensorDataList()) {

                        if (Objects.equals(entry.getN(), "source")) {
                            sysCity.setSource(entry.getSv());
                        } else if (Objects.equals(entry.getN(), "longitude")) {
                            sysCity.setLongitude(entry.getV());
                        } else if (Objects.equals(entry.getN(), "latitude")) {
                            sysCity.setLatitude(entry.getV());
                        } else if (Objects.equals(entry.getN(), "temperature")) {
                            sysCity.setTemperature(entry.getV());
                        } else if (Objects.equals(entry.getN(), "humidity")) {
                            sysCity.setHumidity(entry.getV());
                        } else if (Objects.equals(entry.getN(), "light")) {
                            sysCity.setLight(entry.getV());
                        } else if (Objects.equals(entry.getN(), "dust")) {
                            sysCity.setDust(entry.getV());
                        } else if (Objects.equals(entry.getN(), "airquality_raw")) {
                            sysCity.setAirquality_raw(entry.getV());
                        }
                    }
                }
            } else {
                if (rowToParse == 0) {
                    rowToParse = 1;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error when reading new row " + e);
        }
        rowToParse++;
        return sysCity;
    }
}
