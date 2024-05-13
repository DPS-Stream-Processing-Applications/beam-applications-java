package at.ac.uibk.dps.streamprocessingapplications.genevents.factory;

/** Created by tarun on 28/5/15. */
public class ArgumentClass {
  // String deploymentMode; // Local ('L') or Distributed-cluster ('C') Mode
  String topoName;
  String inputDatasetPathName; // Full path along with File Name
  String experiRunId;

  String outputDirName; // Path where the output log file from spout and sink has to be kept
  String tasksPropertiesFilename;
  String tasksName;
  String bootStrapServerKafka;
  String kafkaTopic;

  String databaseUrl;

  int parallelism;

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public String getDatabaseUrl() {
    return databaseUrl;
  }

  public void setDatabaseUrl(String databaseUrl) {
    this.databaseUrl = databaseUrl;
  }

  public String getBootStrapServerKafka() {
    return bootStrapServerKafka;
  }

  public void setBootStrapServerKafka(String bootStrapServerKafka) {
    this.bootStrapServerKafka = bootStrapServerKafka;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public void setKafkaTopic(String kafkaTopic) {
    this.kafkaTopic = kafkaTopic;
  }

  public String getTasksName() {
    return tasksName;
  }

  public void setTasksName(String tasksName) {
    this.tasksName = tasksName;
  }

  public String getOutputDirName() {
    return outputDirName;
  }

  public void setOutputDirName(String outputDirName) {
    this.outputDirName = outputDirName;
  }

  public String getTopoName() {
    return topoName;
  }

  public void setTopoName(String topoName) {
    this.topoName = topoName;
  }

  public String getInputDatasetPathName() {
    return inputDatasetPathName;
  }

  public void setInputDatasetPathName(String inputDatasetPathName) {
    this.inputDatasetPathName = inputDatasetPathName;
  }

  public String getExperiRunId() {
    return experiRunId;
  }

  public void setExperiRunId(String experiRunId) {
    this.experiRunId = experiRunId;
  }

  public String getTasksPropertiesFilename() {
    return tasksPropertiesFilename;
  }

  public void setTasksPropertiesFilename(String tasksPropertiesFilename) {
    this.tasksPropertiesFilename = tasksPropertiesFilename;
  }

  @Override
  public String toString() {
    return "ArgumentClass{"
        + "topoName='"
        + topoName
        + '\''
        + ", inputDatasetPathName='"
        + inputDatasetPathName
        + '\''
        + ", experiRunId='"
        + experiRunId
        + '\''
        + ", outputDirName='"
        + outputDirName
        + '\''
        + ", tasksPropertiesFilename='"
        + tasksPropertiesFilename
        + '\''
        + ", tasksName='"
        + tasksName
        + '\''
        + ", bootStrapServerKafka='"
        + bootStrapServerKafka
        + '\''
        + ", kafkaTopic='"
        + kafkaTopic
        + '\''
        + ", databaseUrl='"
        + databaseUrl
        + '\''
        + '}';
  }
}
