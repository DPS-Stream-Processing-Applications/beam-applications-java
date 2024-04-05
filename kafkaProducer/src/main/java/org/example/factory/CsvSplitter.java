package org.example.factory;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;

/**
 * Splits the CSV file in round-robin manner and stores it to individual files based on the number
 * of threads
 */
public class CsvSplitter {

    public static int numThreads;
    public static int peakRate;

    public static List<String> extractHeadersFromCSV(String inputFileName) {
        try {
            CSVReader reader = new CSVReader(new FileReader(inputFileName));
            String[] headers = reader.readNext(); // use .intern() later
            reader.close();
            List<String> headerList = new ArrayList<>();
            for (String s : headers) {
                headerList.add(s);
            }
            return headerList;
        } catch (IOException | CsvValidationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    // Assumes sorted on timestamp csv file
    // It also treats the first event to be at 0 relative time
    public static List<TableClass> roundRobinSplitCsvToMemory(
            String inputSortedCSVFileName, int numThreads, double accFactor, String datasetType)
            throws IOException, CsvValidationException {

        CSVReader reader = new CSVReader(new FileReader(inputSortedCSVFileName));
        String[] nextLine;
        int ctr = 0;
        String[] headers = reader.readNext(); // use .intern() later
        List<String> headerList = new ArrayList<String>();
        for (String s : headers) {
            headerList.add(s);
        }

        List<TableClass> tableList = new ArrayList<TableClass>();
        for (int i = 0; i < numThreads; i++) {
            TableClass tableClass = new TableClass();
            tableClass.setHeader(headerList);
            tableList.add(tableClass);
        }

        TableClass tableClass = null;
        boolean flag = true;
        long startTs = 0, deltaTs = 0;

        // CODE TO ACCOMODATE PLUG DATASET SPECIAL CASE TO RUN IT FOR 10 MINS
        int numMins = 90000; // Keeping it fixed for current set of experiments
        Double cutOffTimeStamp = 0.0; // int msgs=0;
        while ((nextLine = reader.readNext()) != null) {
            // nextLine[] is an array of values from the line
            // System.out.println(nextLine[0] + "  " + nextLine[1] + "   " +
            // nextLine[2] + "  " + nextLine[3] + "  etc...");

            List<String> row = new ArrayList<String>();
            for (int i = 0; i < nextLine.length; i++) {
                row.add(nextLine[i]);
            }

            tableClass = tableList.get(ctr);
            ctr = (ctr + 1) % numThreads;

            int timestampColIndex = 0;
            DateTime date = null;
            if (datasetType.equals("TAXI")) {
                /*
                timestampColIndex = 3;
                date =
                        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
                                .parseDateTime(nextLine[timestampColIndex]);

                 */
                timestampColIndex = 1;
                date = new DateTime(Long.parseLong(nextLine[timestampColIndex]) * 1000);
            } else if (datasetType.equals("SYS")) {
                /*
                timestampColIndex = 0;
                date =
                        ISODateTimeFormat.dateTimeParser()
                                .parseDateTime(nextLine[timestampColIndex]);
                // date = ISODateTimeFormat.dateTimeParser().parseDateTime(
                //		nextLine[timestampColIndex]);

                 */
                timestampColIndex = 1;
                date = new DateTime(Long.parseLong(nextLine[timestampColIndex]) * 1000);
            } else if (datasetType.equals("PLUG")) {
                timestampColIndex = 1;
                date = new DateTime(Long.parseLong(nextLine[timestampColIndex]) * 1000);
                // date = ISODateTimeFormat.dateTimeParser().parseDateTime(
                //		nextLine[timestampColIndex]);
            } else if (datasetType.equals("FIT")) {
                timestampColIndex = 1;
                long unixEpochMillis = Long.parseLong(nextLine[timestampColIndex]);
                date = new DateTime(unixEpochMillis);
            } else if (datasetType.equals("GRID")) {
                timestampColIndex = 1;
                long unixEpochMillis = Long.parseLong(nextLine[timestampColIndex]);
                date = new DateTime(unixEpochMillis);
            }

            long ts = date.getMillis();
            if (flag) {
                startTs = ts;
                flag = false;
                cutOffTimeStamp =
                        startTs
                                + numMins
                                        * (1.0 / accFactor)
                                        * 60
                                        * 1000; // accFactor is actually the scaling factor or
                // deceleration factor
                // System.out.println("GOTSTART TS : "  + ts + " cut off " + cutOffTimeStamp);
            }

            if (ts > cutOffTimeStamp) {
                // System.out.println("GOT TS : "  + ts + " cut off " + cutOffTimeStamp + "  msgs "
                // + (++msgs));
                break; // No More data to be loaded
            }

            deltaTs = ts - startTs;
            deltaTs = (long) (accFactor * deltaTs);
            tableClass.append(deltaTs, row);
            // System.out.println("ts " + (ts - startTs) + " deltaTs " + deltaTs);
        }

        reader.close();
        return tableList;
    }
}
