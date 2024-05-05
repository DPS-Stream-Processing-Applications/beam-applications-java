package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.kafka.MyKafkaConsumer;
import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.types.MqttSubscribeEntry;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.functions.SourceFn.checkDataSetType;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.MQTT_SUBSCRIBE_ENTRY_JSON_TYPE;

public class MqttSubscribeFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/mqttSubscribe");

    static final TypeName INBOX = TypeName.typeNameFromString("pred/blobRead");

    private static final ValueSpec<Long> MSGID_COUNT = ValueSpec
            .named("message_counter")
            .withLongType();


    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withValueSpec(MSGID_COUNT)
                    .withSupplier(MqttSubscribeFn::new)
                    .build();


    private static long msgid = 1;
    private static Logger l;
    MyKafkaConsumer myKafkaConsumer;
    String spoutLogFileName = null;
    Properties p;

    String bootStrapServer;
    String topic;

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
        this.bootStrapServer = "kafka-cluster-kafka-bootstrap.kafka.svc:9092";
        this.topic = "pred-topic-1";
        initLogger(LoggerFactory.getLogger("APP"));
        myKafkaConsumer =
                new MyKafkaConsumer(bootStrapServer, "group-" + UUID.randomUUID(), 1000, topic);
        myKafkaConsumer.setup(l, p);
    }


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        String rowString = new String(message.rawValue().toByteArray(), StandardCharsets.UTF_8);
        String datasetType = checkDataSetType("{\"e\":" + rowString + ",\"bt\": \"1358101800000\"}");
        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, "dummy");
        myKafkaConsumer.doTask(map);
        String arg1 = myKafkaConsumer.getLastResult();
        // arg1 = "test-12";

        if (arg1 != null) {
            try {
                msgid++;
                // ba.batchLogwriter(System.nanoTime(),"MSGID," + msgId);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Exception in processElement of MqttBeam " + e);
            }
            if (l.isInfoEnabled()) l.info("arg1 in MQTTSubscribeSpout {}", arg1);
            MqttSubscribeEntry mqttSubscribeEntry = new MqttSubscribeEntry(arg1.split("-")[1], arg1, Long.toString(msgid), datasetType);
            context.send(
                    MessageBuilder.forAddress(INBOX, String.valueOf(mqttSubscribeEntry.getMsgid()))
                            .withCustomType(MQTT_SUBSCRIBE_ENTRY_JSON_TYPE, mqttSubscribeEntry)
                            .build());

        }
        return context.done();
    }
}
