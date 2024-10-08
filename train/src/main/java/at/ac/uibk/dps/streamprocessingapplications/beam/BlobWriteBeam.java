package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.BlobUploadEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.TrainEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AzureBlobUploadTask;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobWriteBeam extends DoFn<TrainEntry, BlobUploadEntry> {

  private static Logger l;
  AzureBlobUploadTask azureBlobUploadTask;
  String baseDirname = "";
  String fileName = "T";
  String datasetName = "";
  private Properties p;

  public BlobWriteBeam(Properties p_) {
    p = p_;
  }

  public static void initLogger(Logger l_) {
    l = l_;
  }

  @Setup
  public void setup() throws IOException {
    initLogger(LoggerFactory.getLogger("APP"));
    azureBlobUploadTask = new AzureBlobUploadTask();
    baseDirname = p.getProperty("IO.AZURE_BLOB_UPLOAD.DIR_NAME");
    datasetName = p.getProperty("TRAIN.DATASET_NAME");
    azureBlobUploadTask.setup(l, p);
  }

  @ProcessElement
  public void processElement(@Element TrainEntry input, OutputReceiver<BlobUploadEntry> out)
      throws IOException {
    String res = "0";
    String msgId = input.getMsgid();

    fileName = input.getFileName();
    String filepath = baseDirname + fileName;

    if (l.isInfoEnabled()) l.info("filepath in upload bolt{} and name is {}", filepath, fileName);

    HashMap<String, String> map = new HashMap<>();
    map.put(AbstractTask.DEFAULT_KEY, filepath);

    Float blobRes = azureBlobUploadTask.doTask(map);

    if (res != null) {
      if (blobRes != Float.MIN_VALUE) {
        BlobUploadEntry blobUploadEntry = new BlobUploadEntry(msgId, fileName);
        blobUploadEntry.setArrivalTime(input.getArrivalTime());
        out.output(blobUploadEntry);
      } else {
        if (l.isWarnEnabled()) l.warn("Error in AzureBlobUploadTaskBolt");
        throw new RuntimeException();
      }
    }
  }
}
