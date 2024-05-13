package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.BlobReadEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.MqttSubscribeEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AzureBlobDownloadTask;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobReadBeam extends DoFn<MqttSubscribeEntry, BlobReadEntry> {

  private static Logger l;
  Properties p;
  // String csvFileNameOutSink; // Full path name of the file at the sink bolt
  AzureBlobDownloadTask azureBlobDownloadTask;

  public BlobReadBeam(Properties p_) {
    // this.csvFileNameOutSink = csvFileNameOutSink;
    p = p_;
  }

  public static void initLogger(Logger l_) {
    l = l_;
  }

  @Setup
  public void prepare() {
    azureBlobDownloadTask = new AzureBlobDownloadTask();
    initLogger(LoggerFactory.getLogger("APP"));
    azureBlobDownloadTask.setup(l, p);
  }

  @ProcessElement
  public void processElement(@Element MqttSubscribeEntry input, OutputReceiver<BlobReadEntry> out)
      throws IOException {
    // path for both model files
    String BlobModelPath = input.getBlobModelPath();
    String analyticsType = input.getAnalaytictype();

    String msgId = input.getMsgid();

    //        azureBlobDownloadTask.doTask(rowString);
    HashMap<String, String> map = new HashMap<>();

    map.put(AbstractTask.DEFAULT_KEY, BlobModelPath);
    azureBlobDownloadTask.doTask(map);
    byte[] BlobModelObject = azureBlobDownloadTask.getLastResult();

    if (l.isInfoEnabled())
      l.info("downloaded updated model file {} with size {}", BlobModelPath, 23);

    // FIXME: read and emit model for DTC
    out.output(new BlobReadEntry(BlobModelObject, msgId, "modelupdate", analyticsType, "meta"));
  }
}
