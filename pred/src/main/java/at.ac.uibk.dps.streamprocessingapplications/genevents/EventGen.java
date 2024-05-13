package at.ac.uibk.dps.streamprocessingapplications.genevents;

import at.ac.uibk.dps.streamprocessingapplications.beam.ISyntheticEventGen;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.CsvSplitter;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.JsonSplitter;
import at.ac.uibk.dps.streamprocessingapplications.genevents.factory.TableClass;
import at.ac.uibk.dps.streamprocessingapplications.genevents.utils.GlobalConstants;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class EventGen {
  ISyntheticEventGen iseg;
  ExecutorService executorService;
  double scalingFactor;

  public EventGen(ISyntheticEventGen iseg) {
    this(iseg, GlobalConstants.accFactor);
  }

  public EventGen(ISyntheticEventGen iseg, double scalingFactor) {
    this.iseg = iseg;
    this.scalingFactor = scalingFactor;
  }

  public static List<String> getHeadersFromCSV(String csvFileName) {
    return CsvSplitter.extractHeadersFromCSV(csvFileName);
  }

  public void launch(String csvFileName, String outCSVFileName) {
    launch(csvFileName, outCSVFileName, -1);
  }

  // Launches all the threads
  public void launch(String csvFileName, String outCSVFileName, long experimentDurationMillis) {
    // 1. Load CSV to in-memory data structure
    // 2. Assign a thread with (new SubEventGen(myISEG, eventList))
    // 3. Attach this thread to ThreadPool
    try {
      int numThreads = GlobalConstants.numThreads;
      // double scalingFactor = GlobalConstants.accFactor;
      String datasetType = "";
      if (outCSVFileName.indexOf("TAXI") != -1) {
        datasetType = "TAXI"; // GlobalConstants.dataSetType = "TAXI";
      } else if (outCSVFileName.indexOf("SYS") != -1) {
        datasetType = "SYS"; // GlobalConstants.dataSetType = "SYS";
      } else if (outCSVFileName.indexOf("PLUG") != -1) {
        datasetType = "PLUG"; // GlobalConstants.dataSetType = "PLUG";
      } else if (outCSVFileName.indexOf("FIT") != -1) {
        datasetType = "FIT";
      } else if (outCSVFileName.indexOf("GRID") != -1) {
        datasetType = "GRID";
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
        subEventGenArr[i] = new SubEventGen(this.iseg, nestedList.get(i), sem1, sem2);
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
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  /***
   * not used
   * @param csvFileName
   * @param outCSVFileName
   * @param experimentDurationMillis
   * @param isJson
   */
  public void launch(
      String csvFileName, String outCSVFileName, long experimentDurationMillis, boolean isJson) {
    // 1. Load CSV to in-memory data structure
    // 2. Assign a thread with (new SubEventGen(myISEG, eventList))
    // 3. Attach this thread to ThreadPool
    if (!isJson) {
      this.launch(csvFileName, outCSVFileName, experimentDurationMillis);
    } else {
      try {
        int numThreads = GlobalConstants.numThreads;
        // double scalingFactor = GlobalConstants.accFactor;
        String datasetType = "";
        if (outCSVFileName.contains("TAXI")) {
          datasetType = "TAXI"; // GlobalConstants.dataSetType = "TAXI";
        } else if (outCSVFileName.contains("SYS")) {
          datasetType = "SYS"; // GlobalConstants.dataSetType = "SYS";
        } else if (outCSVFileName.contains("PLUG")) {
          datasetType = "PLUG"; // GlobalConstants.dataSetType = "PLUG";
        } else if (outCSVFileName.contains("SENML")) {
          datasetType = "SENML"; // GlobalConstants.dataSetType = "PLUG";
        }

        List<TableClass> nestedList =
            JsonSplitter.roundRobinSplitJsonToMemory(
                csvFileName, numThreads, scalingFactor, datasetType);
        this.executorService = Executors.newFixedThreadPool(numThreads);
        Semaphore sem1 = new Semaphore(0);

        Semaphore sem2 = new Semaphore(0);

        SubEventGen[] subEventGenArr = new SubEventGen[numThreads];
        for (int i = 0; i < numThreads; i++) {
          // this.executorService.execute(new SubEventGen(this.iseg, nestedList.get(i)));
          subEventGenArr[i] = new SubEventGen(this.iseg, nestedList.get(i), sem1, sem2);
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
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException("Error in launching EventGen " + e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

class SubEventGen implements Runnable {
  ISyntheticEventGen iseg;
  TableClass eventList;
  Long experiStartTime; // in millis since epoch
  Semaphore sem1, sem2;
  Long experiDuration = -1L;

  public SubEventGen(
      ISyntheticEventGen iseg, TableClass eventList, Semaphore sem1, Semaphore sem2) {
    this.iseg = iseg;
    this.eventList = eventList;
    this.sem1 = sem1;
    this.sem2 = sem2;
  }

  @Override
  public void run() {

    sem1.release();
    try {
      sem2.acquire();
    } catch (InterruptedException e1) {
      e1.printStackTrace();
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
        // delay = 1000;
        if (delay > 10) { // sleep only if it is non-trivial time. We will catch up on sleep
          // later.
          try {
            System.out.println("Sleeping for " + delay);
            Thread.sleep(delay);
          } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
        this.iseg.receive(event);

        currentRuntime =
            (currentTs - experiStartTime) + delay; // appox time since the experiment started
      }

      experiRestartTime = System.currentTimeMillis();
    } while (!runOnce && (currentRuntime < experiDuration));
  }
}
