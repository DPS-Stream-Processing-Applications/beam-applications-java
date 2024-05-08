package org.apache.flink.statefun.playground.java.greeter.utils;


import org.apache.flink.statefun.playground.java.greeter.types.azure.GRID_data;

import java.util.Random;

// FIXME: Implement real logic
public class GridDataGenerator {

  public static GRID_data generateRandomGridData() {
    GRID_data gridData = new GRID_data();
    Random random = new Random();

    gridData.setMeterid(String.valueOf(random.nextDouble()));
    gridData.setTs(String.valueOf(random.nextDouble()));
    gridData.setEnergyconsumed(String.valueOf(random.nextDouble()));

    return gridData;
  }
}
