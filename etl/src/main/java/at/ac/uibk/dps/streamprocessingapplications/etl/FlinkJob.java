package at.ac.uibk.dps.streamprocessingapplications.etl;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.cli.*;

public class FlinkJob {
  /* HACK:
   * This is a workaround to handle the `missing property named `experiRunId` error.
   * The error occurs because it should be possible to set pipeline options through the CLI as well as
   * set the custom `experiRunId` option for the job configuration. Beam will try to interpret the `experiRunId`
   * option as one of the `FlinkPipelineOptions`.
   */
  public interface FlinkPipelineOptionsExperiRunId extends FlinkPipelineOptions {
    @Description("The experiment run ID.")
    String getExperiRunId();

    void setExperiRunId(String value);
  }

  public static void main(String[] args) {
    Options cliOptions = new Options();
    Option experiRunIdOption =
        new Option("eri", "experiRunId", true, "The Dataset the job should parse.");
    experiRunIdOption.setRequired(true);
    experiRunIdOption.setType(String.class);
    cliOptions.addOption(experiRunIdOption);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;
    String experiRunId = null;

    try {
      cmd = parser.parse(cliOptions, args);
      experiRunId = cmd.getOptionValue("experiRunId");
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("FlinkJob", cliOptions);
      System.exit(1);
    }

    FlinkPipelineOptionsExperiRunId pipelineOptions =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(FlinkPipelineOptionsExperiRunId.class);
    pipelineOptions.setRunner(FlinkRunner.class);
    pipelineOptions.setJobName("ETL");
    Pipeline pipeline;

    switch (experiRunId) {
      case "TAXI":
        pipeline = PipelineBuilder.buildTAXIPipeline(pipelineOptions);
        break;
      case "FIT":
        pipeline = PipelineBuilder.buildFITPipeline(pipelineOptions);
        break;
      case "GRID":
        pipeline = PipelineBuilder.buildGRIDPipeline(pipelineOptions);
        break;
      default:
        throw new RuntimeException("Pipeline data type not recognised.");
    }
    pipeline.run();
  }
}
