package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.SourceEntry;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceBeam extends DoFn<String, SourceEntry> implements ISyntheticEventGen {

  private static Logger l;

  BlockingQueue<List<String>> eventQueue;
  String csvFileName;
  String outSpoutCSVLogFileName;
  String experiRunId;
  long msgId;
  private final String datasetType;

  public static void initLogger(Logger l_) {
    l = l_;
  }

  public SourceBeam(
      String csvFileName, String outSpoutCSVLogFileName, String experiRunId, String datasetType) {
    this.csvFileName = csvFileName;
    this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
    this.experiRunId = experiRunId;
    this.datasetType = datasetType;
  }

  public SourceBeam(String csvFileName, String outSpoutCSVLogFileName, String datasetType) {
    this(csvFileName, outSpoutCSVLogFileName, "", datasetType);
  }

  private long extractTimeStamp(String row) {
    Gson gson = new Gson();
    JsonArray jsonArray = gson.fromJson(row, JsonArray.class);

    String pickupDatetime = null;
    for (int i = 0; i < jsonArray.size(); i++) {
      JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
      if (jsonObject.has("n") && jsonObject.get("n").getAsString().equals("pickup_datetime")) {
        pickupDatetime = jsonObject.get("vs").getAsString();
        break;
      }
    }

    return convertToUnixTimeStamp(pickupDatetime);
  }

  private long convertToUnixTimeStamp(String dateString) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    long unixTimestampSeconds = 0;
    try {
      Date date = dateFormat.parse(dateString);
      long unixTimestamp = date.getTime();
      unixTimestampSeconds = unixTimestamp / 1000;
    } catch (ParseException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return unixTimestampSeconds;
  }

  @Setup
  public void setup() {
    Random r = new Random();
    try {
      msgId = (long) (1 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));

    } catch (Exception e) {

      e.printStackTrace();
    }
    initLogger(LoggerFactory.getLogger("APP"));
  }

  @ProcessElement
  public void processElement(@Element String input, OutputReceiver<SourceEntry> out)
      throws IOException {
    try {
      SourceEntry values = new SourceEntry();
      String rowString = input;
      String newRow;
      if (datasetType.equals("TAXI")) {
        newRow = "{\"e\":" + rowString + ",\"bt\":" + extractTimeStamp(rowString) + "}";
      } else if (datasetType.equals("FIT")) {
        newRow = "{\"e\":" + rowString + ",\"bt\": \"1358101800000\"}";

      } else {
        newRow = "{\"e\":" + rowString + ",\"bt\":1358101800000}";
      }
      l.info(newRow);
      values.setMsgid(Long.toString(msgId));
      values.setPayLoad(newRow);
      out.output(values);
      msgId++;
    } catch (Exception e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void receive(List<String> event) {
    try {
      this.eventQueue.put(event);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
