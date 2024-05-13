package at.ac.uibk.dps.streamprocessingapplications.entity.azure;

public class SensorData {
  private String u;
  private String n;
  private String v;

  private String sv;

  public String getU() {
    return u;
  }

  public void setU(String u) {
    this.u = u;
  }

  public String getN() {
    return n;
  }

  public void setN(String n) {
    this.n = n;
  }

  public String getV() {
    return v;
  }

  public void setV(String v) {
    this.v = v;
  }

  public String getSv() {
    return sv;
  }

  public void setSv(String sv) {
    this.sv = sv;
  }

  @Override
  public String toString() {
    return "SensorData{"
        + "u='"
        + u
        + '\''
        + ", n='"
        + n
        + '\''
        + ", v='"
        + v
        + '\''
        + ", sv='"
        + sv
        + '\''
        + '}';
  }
}
