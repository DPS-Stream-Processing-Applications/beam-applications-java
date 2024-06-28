package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.kafka.MyKafkaProducer;
import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.types.BlobUploadEntry;
import org.apache.flink.statefun.playground.java.greeter.types.MqttPublishEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.BlobUpload_ENTRY_JSON_TYPE;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.MqttPublish_ENTRY_JSON_TYPE;

public class MqttPublishTrainFn implements StatefulFunction {
    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/mqttPublishTrain");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(MqttPublishTrainFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/sinkTrain");
    private static Logger l;
    MyKafkaProducer myKafkaProducer;
    String server;
    String topic;
    private Properties p;

    public static void initLogger(Logger l_) {
        l = l_;
    }


    public void setup() {
        p = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get("/resources/all_tasks.properties"))) {
            p.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.server = "kafka-cluster-kafka-bootstrap.default.svc:9092";
        this.topic = "train-topic-2";
        initLogger(LoggerFactory.getLogger("APP"));
        myKafkaProducer = new MyKafkaProducer(server, topic, p);
        myKafkaProducer.setup(l, p);
    }


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        setup();
        BlobUploadEntry blobUploadEntry = message.as(BlobUpload_ENTRY_JSON_TYPE);
        String msgId = blobUploadEntry.getMsgid();
        String filename = blobUploadEntry.getFileName();

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, filename);
        Float res = 93f;
        myKafkaProducer.doTask(map);
        MqttPublishEntry mqttPublishEntry = new MqttPublishEntry(msgId);
        mqttPublishEntry.setArrivalTime(blobUploadEntry.getArrivalTime());
        context.send(
                MessageBuilder.forAddress(INBOX, String.valueOf(mqttPublishEntry.getMsgid()))
                        .withCustomType(MqttPublish_ENTRY_JSON_TYPE, mqttPublishEntry)
                        .build());

        return context.done();
    }
}

