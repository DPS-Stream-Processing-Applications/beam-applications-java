package org.example;

import com.opencsv.exceptions.CsvValidationException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.example.factory.CsvSplitter;
import org.example.factory.JsonSplitter;
import org.example.factory.TableClass;
import org.example.utils.GlobalConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventGen {
  ExecutorService executorService;
  double scalingFactor;

  Producer producer;

  String topic;

  long repetitions;
  long duplication;

  private static Logger l;

  public EventGen(
      double scalingFactor, Producer producer, String topic, long repetitions, long duplication) {
    this.scalingFactor = scalingFactor;
    this.producer = producer;
    this.topic = topic;
    this.repetitions = repetitions;
    this.duplication = duplication;
    initLogger(LoggerFactory.getLogger("APP"));
  }

  public static void initLogger(Logger l_) {
    l = l_;
  }

  public void launch(String csvFileName) {
    launch(csvFileName, -1);
  }

  public void launch(String csvFileName, boolean isJson) {
    launch(csvFileName, -1, isJson);
  }

  // Launches all the threads
  public void launch(String csvFileName, long experimentDurationMillis) {
    // 1. Load CSV to in-memory data structure
    // 2. Assign a thread with (new SubEventGen(myISEG, eventList))
    // 3. Attach this thread to ThreadPool
    try {
      int numThreads = GlobalConstants.numThreads;
      // double scalingFactor = GlobalConstants.accFactor;
      String datasetType = getString(csvFileName);

      List<TableClass> nestedList =
          CsvSplitter.roundRobinSplitCsvToMemory(
              csvFileName, numThreads, scalingFactor, datasetType);

      long counter = 0;
      while (counter < (repetitions + 1)) {

        this.executorService = Executors.newFixedThreadPool(numThreads);

        Semaphore sem1 = new Semaphore(0);

        Semaphore sem2 = new Semaphore(0);

        SubEventGen[] subEventGenArr = new SubEventGen[numThreads];

        for (int i = 0; i < numThreads; i++) {
          // this.executorService.execute(new SubEventGen(this.iseg, nestedList.get(i)));
          subEventGenArr[i] =
              new SubEventGen(
                  nestedList.get(i), sem1, sem2, producer, numThreads, topic, duplication, l);
          this.executorService.execute(subEventGenArr[i]);
        }

        sem1.acquire(numThreads);
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
        try {
          if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
            executorService.shutdownNow();
          }
        } catch (InterruptedException e) {
          executorService.shutdownNow();
        }

        counter++;
      }

    } catch (InterruptedException | IOException | CsvValidationException e) {
      l.warn(e.toString());
      throw new RuntimeException(e);
    }
  }

  private static String getString(String csvFileName) {
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
    return datasetType;
  }

  public void launch(String csvFileName, long experimentDurationMillis, boolean isJson) {
    // 1. Load CSV to in-memory data structure
    // 2. Assign a thread with (new SubEventGen(myISEG, eventList))
    // 3. Attach this thread to ThreadPool
    try {
      int numThreads = GlobalConstants.numThreads;
      // double scalingFactor = GlobalConstants.accFactor;
      String datasetType = "";
      if (csvFileName.contains("TAXI")) {
        datasetType = "TAXI"; // GlobalConstants.dataSetType = "TAXI";
      } else if (csvFileName.contains("SYS")) {
        datasetType = "SYS"; // GlobalConstants.dataSetType = "SYS";
      } else if (csvFileName.contains("FIT")) {
        datasetType = "FIT"; // GlobalConstants.dataSetType = "PLUG";
      }
      List<TableClass> nestedList =
          JsonSplitter.roundRobinSplitJsonToMemory(
              csvFileName, numThreads, scalingFactor, datasetType);

      long counter = 0;
      while (counter < (repetitions + 1)) {
        this.executorService = Executors.newFixedThreadPool(numThreads);
        Semaphore sem1 = new Semaphore(0);

        Semaphore sem2 = new Semaphore(0);

        SubEventGen[] subEventGenArr = new SubEventGen[numThreads];
        for (int i = 0; i < numThreads; i++) {
          // this.executorService.execute(new SubEventGen(this.iseg, nestedList.get(i)));
          subEventGenArr[i] =
              new SubEventGen(
                  nestedList.get(i), sem1, sem2, producer, numThreads, topic, duplication, l);
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
        try {
          if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
            executorService.shutdownNow();
          }
        } catch (InterruptedException e) {
          executorService.shutdownNow();
        }

        counter++;
      }

    } catch (InterruptedException e) {
      l.warn(e.toString());
      throw new RuntimeException("Error in launching EventGen " + e);
    } catch (IOException | CsvValidationException e) {
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

  long duplication;

  private static Logger l;

  public SubEventGen(
      TableClass eventList,
      Semaphore sem1,
      Semaphore sem2,
      Producer producer,
      long numThreads,
      String topic,
      long duplication,
      Logger logger) {
    this.eventList = eventList;
    this.sem1 = sem1;
    this.sem2 = sem2;
    this.producer = producer;
    this.numThreads = numThreads;
    this.topic = topic;
    this.duplication = duplication;
    l = logger;
  }

  @Override
  public void run() {
    sem1.release();
    try {
      sem2.acquire();
    } catch (InterruptedException e1) {
      l.warn(e1.toString());
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
            deltaTs - (currentTs - experiRestartTime); // how long until this event should be
        // sent?
        if (delay > 10) {
          try {
            System.out.println("delay: " + delay);
            Thread.sleep(delay);
          } catch (InterruptedException e) {
            l.warn(e.toString());
          }
        }
        StringBuilder rowStringBuf = new StringBuilder();
        for (String s : event) {
          rowStringBuf.append(",").append(s);
        }
        String rowString = rowStringBuf.substring(1);
        try {
          for (long j = 0; j < (duplication + 1); j++) {
            producer.run(rowString, topic);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        currentRuntime =
            (currentTs - experiStartTime) + delay; // appox time since the experiment started
      }

      experiRestartTime = System.currentTimeMillis();
    } while (!runOnce && (currentRuntime < experiDuration));
    sem2.release();
  }
}
