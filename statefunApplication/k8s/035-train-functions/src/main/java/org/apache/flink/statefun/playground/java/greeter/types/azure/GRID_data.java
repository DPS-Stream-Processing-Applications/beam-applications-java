package org.apache.flink.statefun.playground.java.greeter.types.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public final class GRID_data extends TableServiceEntity {
  private String meterid, ts, energyconsumed;
  private long rangeTs;

  public long getRangeTs() {
    return rangeTs;
  }

  public void setRangeTs(long rangeTs) {
    this.rangeTs = rangeTs;
  }

  public String getTs() {
    return ts;
  }

  public void setTs(String ts) {
    this.ts = ts;
  }

  public String getMeterid() {
    return meterid;
  }

  public void setMeterid(String meterid) {
    this.meterid = meterid;
  }

  public String getEnergyconsumed() {
    return energyconsumed;
  }

  public void setEnergyconsumed(String energyconsumed) {
    this.energyconsumed = energyconsumed;
  }
}
