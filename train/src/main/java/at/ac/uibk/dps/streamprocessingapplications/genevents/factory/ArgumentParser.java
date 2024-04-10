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

        if (params.getNumberOfParameters() != 6) {
            System.out.println("invalid number of arguments");
            return null;
        } else {
            ArgumentClass argumentClass = new ArgumentClass();
            argumentClass.setTopoName(params.get("topoName"));
            argumentClass.setExperiRunId(params.get("experiRunId"));
            argumentClass.setDatabaseUrl(params.get("databaseUrl"));
            argumentClass.setTasksName(params.get("taskName"));
            argumentClass.setBootStrapServerKafka(params.get("bootstrap"));
            argumentClass.setKafkaTopic(params.get("topic"));
            return argumentClass;
        }
    }
}
