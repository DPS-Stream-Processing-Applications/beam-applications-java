package org.apache.flink.statefun.playground.java.greeter.tasks;

import org.slf4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Downloads a blob from Azure cloud to local memory
 *
 * <p>This task is thread-safe, and can be run from multiple threads.
 *
 * @author shukla, simmhan
 */
public class AzureBlobDownloadTask extends AbstractTask<String, byte[]> {

    // static fields common to all threads
    private final Object SETUP_LOCK = new Object();
    private boolean doneSetup = false;
    private int useMsgField;

    private String[] fileNames;


    public void setup(Logger l_, Properties p_) {
        super.setup(l_, p_);
        synchronized (SETUP_LOCK) {
            if (!doneSetup) { // Do setup only once for this task
                useMsgField = Integer.parseInt(p_.getProperty("IO.AZURE_BLOB_DOWNLOAD.USE_MSG_FIELD", "0"));

                String storageConnStr = p_.getProperty("IO.AZURE_STORAGE_CONN_STR");
                String containerName = p_.getProperty("IO.AZURE_BLOB.CONTAINER_NAME");
                String csvFileNames =
                        p_.getProperty("IO.AZURE_BLOB_DOWNLOAD.FILE_NAMES"); // multiple CSV files names
                assert csvFileNames != null;
                fileNames = csvFileNames.split(",");
                doneSetup = true;
            }
        }
    }

    @Override
    protected Float doTaskLogic(Map<String, String> map) {
        String m = map.get(AbstractTask.DEFAULT_KEY);
        // get file index to be downloaded from message or at random
        int fileindex;
        if (useMsgField > 0) {
            fileindex = Integer.parseInt(m.split(",")[useMsgField - 1]) % fileNames.length;
        } else {
            fileindex = ThreadLocalRandom.current().nextInt(fileNames.length);
        }

        String fileName = fileNames[fileindex];
        if (useMsgField == 0) {
            fileName = m; // getting file name from mqttpublish
        }

        //		fileName = "peakRateBarplot.pdf";
        // CloudBlob cloudBlob = connectToAzBlob(storageConnStr, containerName, fileName, l);
        // assert cloudBlob != null;
        // int result = getAzBlob(cloudBlob, l);
        int result = 1;
        return (float) result;
    }
}
