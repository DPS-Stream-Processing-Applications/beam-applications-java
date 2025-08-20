package at.ac.uibk.dps.streamprocessingapplications.riotbenchsinglejob;

import at.ac.uibk.dps.streamprocessingapplications.train.genevents.factory.PredCustomOptions;
import java.util.Arrays;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.cli.*;

public class FlinkJobFIT {

  public static void main(String[] args) {
    Options cliOptions = new Options();

    Option databaseUrlOption =
        new Option("db", "databaseUrl", true, "The database connection URL.");
    databaseUrlOption.setRequired(true);
    databaseUrlOption.setType(String.class);
    cliOptions.addOption(databaseUrlOption);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;
    String databaseUrl = null;

    String[] filteredArgs =
        Arrays.stream(args)
            .filter(a -> a.startsWith("-db") || a.startsWith("--databaseUrl"))
            .toArray(String[]::new);

    try {
      cmd = parser.parse(cliOptions, filteredArgs);
      databaseUrl = cmd.getOptionValue("databaseUrl");
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("FlinkJob", cliOptions);
      System.exit(1);
    }

    PredCustomOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PredCustomOptions.class);
    pipelineOptions.setRunner(FlinkRunner.class);
    pipelineOptions.setLatencyTrackingInterval(5L);
    pipelineOptions.setDatabaseUrl(databaseUrl);
    pipelineOptions.setJobName("RIOT");
    pipelineOptions.setAttachedMode(false); // INFO: Required to deploy as Application Cluster

    Pipeline pipeline = PipelineBuilder.buildFITPipeline(pipelineOptions);
    pipeline.run();
  }
}
