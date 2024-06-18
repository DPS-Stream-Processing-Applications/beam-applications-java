package at.ac.uibk.dps.streamprocessingapplications.genevents.factory;

import org.apache.flink.api.java.utils.ParameterTool;

/** Created by tarun on 28/5/15. */
public class ArgumentParser {

  /*
  Convention is:
  Command Meaning: topology-fully-qualified-name <local-or-cluster> <Topo-name> <input-dataset-path-name> <Experi-Run-id> <scaling-factor>
  Example command: SampleTopology L NA /var/tmp/bangalore.csv E01-01 0.001
   */
  public static ArgumentClass parserCLI(String[] args) {
    ParameterTool params = ParameterTool.fromArgs(args);

    if (params.getNumberOfParameters() < 2) {
      System.out.println("invalid number of arguments " + params.getNumberOfParameters());
      return null;
    } else {
      ArgumentClass argumentClass = new ArgumentClass();
      for (String argument : args) {
        String[] splitArgument = argument.split("=");
        if (splitArgument[0].equals("--experiRunId")) {
          argumentClass.setExperiRunId(splitArgument[1]);
        } else if (splitArgument[0].equals("--databaseUrl")) {
          argumentClass.setDatabaseUrl(splitArgument[1]);
        }
      }
      argumentClass.setTopoName("IdentityTopology");
      argumentClass.setTasksName("bench");
      argumentClass.setBootStrapServerKafka("kafka-cluster-kafka-bootstrap:9092");
      argumentClass.setKafkaTopic("train-source");
      return argumentClass;
    }
  }
}
