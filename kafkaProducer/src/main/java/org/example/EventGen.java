package org.example;

import com.opencsv.exceptions.CsvValidationException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.example.factory.CsvSplitter;
import org.example.factory.TableClass;
import org.example.utils.GlobalConstants;

public class EventGen {
    ExecutorService executorService;
    double scalingFactor;

    Producer producer;

    String topic;

    public EventGen(double scalingFactor, Producer producer, String topic) {
        this.scalingFactor = scalingFactor;
        this.producer = producer;
        this.topic = topic;
    }

    public void launch(String csvFileName) {
        launch(csvFileName, -1);
    }

    // Launches all the threads
    public void launch(String csvFileName, long experimentDurationMillis) {
        // 1. Load CSV to in-memory data structure
        // 2. Assign a thread with (new SubEventGen(myISEG, eventList))
        // 3. Attach this thread to ThreadPool
        try {
            int numThreads = GlobalConstants.numThreads;
            // double scalingFactor = GlobalConstants.accFactor;
            String datasetType = "";
            if (csvFileName.contains("TAXI")) {
                datasetType = "TAXI"; // GlobalConstants.dataSetType = "TAXI";
            } else if (csvFileName.contains("SYS") | csvFileName.contains("CITY")) {
                datasetType = "SYS"; // GlobalConstants.dataSetType = "SYS";
            } else if (csvFileName.contains("PLUG")) {
                datasetType = "PLUG"; // GlobalConstants.dataSetType = "PLUG";
            } else if (csvFileName.contains("FIT")) {
                datasetType = "FIT";
            }

            List<TableClass> nestedList =
                    CsvSplitter.roundRobinSplitCsvToMemory(
                            csvFileName, numThreads, scalingFactor, datasetType);

            this.executorService = Executors.newFixedThreadPool(numThreads);

            Semaphore sem1 = new Semaphore(0);

            Semaphore sem2 = new Semaphore(0);

            SubEventGen[] subEventGenArr = new SubEventGen[numThreads];

            for (int i = 0; i < numThreads; i++) {
                // this.executorService.execute(new SubEventGen(this.iseg, nestedList.get(i)));
                subEventGenArr[i] =
                        new SubEventGen(nestedList.get(i), sem1, sem2, producer, numThreads, topic);
                this.executorService.execute(subEventGenArr[i]);
            }

            sem1.acquire(numThreads);
            // set the start time to all the thread objects
            long experiStartTs = System.currentTimeMillis();
            for (int i = 0; i < numThreads; i++) {
                // this.executorService.execute(new SubEventGen(this.iseg, nestedList.get(i)));
                subEventGenArr[i].experiStartTime = experiStartTs;
                if (experimentDurationMillis > 0)
                    subEventGenArr[i].experiDuration = experimentDurationMillis;
                this.executorService.execute(subEventGenArr[i]);
            }
            sem2.release(numThreads);

            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        } catch (InterruptedException | IOException | CsvValidationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}

class SubEventGen implements Runnable {
    TableClass eventList;
    Long experiStartTime; // in millis since epoch
    Semaphore sem1, sem2;
    Long experiDuration = -1L;

    Producer producer;

    long counter = 0;

    long numThreads;
    String topic;

    public SubEventGen(
            TableClass eventList,
            Semaphore sem1,
            Semaphore sem2,
            Producer producer,
            long numThreads,
            String topic) {
        this.eventList = eventList;
        this.sem1 = sem1;
        this.sem2 = sem2;
        this.producer = producer;
        this.numThreads = numThreads;
        this.topic = topic;
    }

    @Override
    public void run() {
        sem1.release();
        try {
            sem2.acquire();
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            throw new RuntimeException(e1);
        }
        List<List<String>> rows = this.eventList.getRows();
        int rowLen = rows.size();
        List<Long> timestamps = this.eventList.getTs();
        Long experiRestartTime = experiStartTime;
        boolean runOnce = (experiDuration < 0);
        long currentRuntime = 0;

        do {
            for (int i = 0; i < rowLen && (runOnce || (currentRuntime < experiDuration)); i++) {
                Long deltaTs = timestamps.get(i);
                List<String> event = rows.get(i);
                Long currentTs = System.currentTimeMillis();
                long delay =
                        deltaTs
                                - (currentTs
                                        - experiRestartTime); // how long until this event should be
                // sent?
                if (delay > 10) { // sleep only if it is non-trivial time. We will catch up on sleep
                    // later.
                    try {
                        System.out.println("delay: " + delay);
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                StringBuilder rowStringBuf = new StringBuilder();
                for (String s : event) {
                    rowStringBuf.append(",").append(s);
                }
                String rowString = rowStringBuf.substring(1);
                try {
                    producer.run(rowString, topic);
                    System.out.println(rowString);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                currentRuntime =
                        (currentTs - experiStartTime)
                                + delay; // appox time since the experiment started
            }

            experiRestartTime = System.currentTimeMillis();
        } while (!runOnce && (currentRuntime < experiDuration));
        sem2.release();
    }
}
