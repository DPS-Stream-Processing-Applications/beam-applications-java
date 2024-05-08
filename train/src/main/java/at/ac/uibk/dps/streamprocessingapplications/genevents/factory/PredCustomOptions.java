package at.ac.uibk.dps.streamprocessingapplications.genevents.factory;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface PredCustomOptions extends FlinkPipelineOptions {

  @Description("Enter the dataset with witch the application will run")
  @Validation.Required
  String getExperiRunId();

  void setExperiRunId(String value);

  @Description("Enter datbase connection url")
  @Validation.Required
  String getDatabaseUrl();

  void setDatabaseUrl(String value);
}
