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

        if (args == null || params.getNumberOfParameters() != 8) {
            System.out.println("invalid number of arguments " + params.getNumberOfParameters());
            return null;
        } else {
            ArgumentClass argumentClass = new ArgumentClass();
            argumentClass.setDeploymentMode(params.get("deploymentMode"));
            argumentClass.setTopoName(params.get("topoName"));
            argumentClass.setInputDatasetPathName(params.get("input"));
            argumentClass.setExperiRunId(params.get("experiRunId"));
            argumentClass.setScalingFactor(Double.parseDouble(params.get("scalingFactor")));
            argumentClass.setOutputDirName(params.get("outputDir"));
            argumentClass.setTasksPropertiesFilename(params.get("taskProp"));
            argumentClass.setTasksName(params.get("taskName"));
            return argumentClass;
        }
    }
}
