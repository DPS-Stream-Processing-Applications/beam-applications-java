package at.ac.uibk.dps.streamprocessingapplications.train.utils;

import at.ac.uibk.dps.streamprocessingapplications.train.entity.azure.GRID_data;
import java.util.Random;

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
