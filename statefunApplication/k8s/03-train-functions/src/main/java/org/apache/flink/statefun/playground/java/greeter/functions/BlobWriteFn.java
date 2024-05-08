package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.AzureBlobUploadTask;
import org.apache.flink.statefun.playground.java.greeter.types.BlobUploadEntry;
import org.apache.flink.statefun.playground.java.greeter.types.TrainEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.BlobUpload_ENTRY_JSON_TYPE;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.Train_ENTRY_JSON_TYPE;


public class BlobWriteFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/blobWrite");

    static final TypeName INBOX = TypeName.typeNameFromString("pred/mqttPublishTrain");

    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(BlobWriteFn::new)
                    .build();

    private static Logger l;
    AzureBlobUploadTask azureBlobUploadTask;
    String baseDirname = "";
    String fileName = "T";
    String datasetName = "";
    private Properties p;

    public void setup() throws IOException {
        p = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get("/resources/all_tasks.properties"))) {
            p.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        initLogger(LoggerFactory.getLogger("APP"));
        azureBlobUploadTask = new AzureBlobUploadTask();
        baseDirname = p.getProperty("IO.AZURE_BLOB_UPLOAD.DIR_NAME");
        datasetName = p.getProperty("TRAIN.DATASET_NAME");
        azureBlobUploadTask.setup(l, p);
    }

    public static void initLogger(Logger l_) {
        l = l_;
    }


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        setup();
        TrainEntry trainEntry = message.as(Train_ENTRY_JSON_TYPE);
        String msgId = trainEntry.getMsgid();

        fileName = trainEntry.getFileName();
        String filepath = baseDirname + fileName;


        HashMap<String, String> map = new HashMap<>();
        map.put(AbstractTask.DEFAULT_KEY, filepath);

        Float blobRes = azureBlobUploadTask.doTask(map);
        if (blobRes != null) {
            if (blobRes != Float.MIN_VALUE) {
                BlobUploadEntry blobUploadEntry = new BlobUploadEntry(msgId, fileName, trainEntry.getDataSetType());
                context.send(
                        MessageBuilder.forAddress(INBOX, String.valueOf(blobUploadEntry.getMsgid()))
                                .withCustomType(BlobUpload_ENTRY_JSON_TYPE, blobUploadEntry)
                                .build());
            } else {
                if (l.isWarnEnabled()) l.warn("Error in AzureBlobUploadTaskBolt");
                throw new RuntimeException();
            }
        }
        return context.done();
    }
}
