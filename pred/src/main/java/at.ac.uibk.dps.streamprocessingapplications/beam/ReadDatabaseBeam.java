package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.tasks.ReadFromDatabaseTask;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadDatabaseBeam extends DoFn<String, String> {
  private static Logger l;
  private final String connectionUrl;
  private final String databaseName;
  private Properties p;
  private ReadFromDatabaseTask readFromDatabaseTask;

  public ReadDatabaseBeam(Properties p, String connectionUrl, String databaseName) {
    this.p = p;
    this.connectionUrl = connectionUrl;
    this.databaseName = databaseName;
  }

  public static void initLogger(Logger l_) {
    l = l_;
  }

  @Setup
  public void prepare() {
    readFromDatabaseTask = new ReadFromDatabaseTask(connectionUrl, databaseName);
    initLogger(LoggerFactory.getLogger("APP"));
    readFromDatabaseTask.setup(l, p);
  }

  @ProcessElement
  public void processElement(@Element String input, OutputReceiver<String> out) throws IOException {
    // path for both model files

    //        azureBlobDownloadTask.doTask(rowString);
    HashMap<String, String> map = new HashMap<>();

    map.put("fileName", "test_arff");
    readFromDatabaseTask.doTask(map);
    byte[] file = readFromDatabaseTask.getLastResult();

    l.info("Database-entry: " + new String(file, StandardCharsets.UTF_8));

    out.output(Arrays.toString(file));
  }
}
