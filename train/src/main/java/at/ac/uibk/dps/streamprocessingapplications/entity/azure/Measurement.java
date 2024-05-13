package at.ac.uibk.dps.streamprocessingapplications.entity.azure;

import com.google.gson.annotations.SerializedName;
import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.List;

public class Measurement extends TableServiceEntity {
  private String ts;

  @SerializedName("e")
  private List<SensorData> sensorDataList;

  public String getTs() {
    return ts;
  }

  public void setTs(String ts) {
    this.ts = ts;
  }

  public List<SensorData> getSensorDataList() {
    return sensorDataList;
  }

  public void setSensorDataList(List<SensorData> sensorDataList) {
    this.sensorDataList = sensorDataList;
  }

  @Override
  public String toString() {
    return "Measurement{" + "ts='" + ts + '\'' + ", sensorDataList=" + sensorDataList + '}';
  }
}
