package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.kafka.MyKafkaProducer;
import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.types.DecisionTreeEntry;
import org.apache.flink.statefun.playground.java.greeter.types.ErrorEstimateEntry;
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

import static org.apache.flink.statefun.playground.java.greeter.types.Types.*;

public class MqttPublishFn implements StatefulFunction {
    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/mqttPublish");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(MqttPublishFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/sink");
    private static Logger l;
    MyKafkaProducer myKafkaProducer;
    String server;
    String topic;
    private Properties p;


    public static void initLogger(Logger l_) {
        l = l_;
    }

    public void setup() {
        initLogger(LoggerFactory.getLogger("APP"));
        p = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get("/resources/all_tasks.properties"))) {
            p.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.server = "kafka-cluster-kafka-bootstrap.default.svc:9092";
        this.topic = "pred-publish";
        myKafkaProducer = new MyKafkaProducer(server, topic, p);
        myKafkaProducer.setup(l, p);

    }

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        setup();
        try {
            if (message.is(DECISION_TREE_ENTRY_JSON_TYPE)) {
                DecisionTreeEntry decisionTreeEntry = message.as(DECISION_TREE_ENTRY_JSON_TYPE);
                String msgId = decisionTreeEntry.getMsgid();
                String meta = decisionTreeEntry.getMeta();
                String analyticsType = decisionTreeEntry.getAnalyticType();
                String obsVal = decisionTreeEntry.getObsval();

                StringBuffer temp = new StringBuffer();
                String res;
                if (analyticsType.equals("DTC")) {
                    res = decisionTreeEntry.getRes();
                    temp.append(msgId)
                            .append(",")
                            .append(meta)
                            .append(",")
                            .append(analyticsType)
                            .append(",obsVal:")
                            .append(obsVal)
                            .append(",RES:")
                            .append(res);
                }

                if (l.isInfoEnabled()) l.info("MQTT result:{}", temp);

                HashMap<String, String> map = new HashMap<>();
                map.put(AbstractTask.DEFAULT_KEY, String.valueOf(temp));
                myKafkaProducer.doTask(map);
                MqttPublishEntry mqttPublishEntry = new MqttPublishEntry(msgId, meta, obsVal, decisionTreeEntry.getDataSetType());
                mqttPublishEntry.setArrivalTime(decisionTreeEntry.getArrivalTime());
                context.send(
                        MessageBuilder.forAddress(INBOX, String.valueOf(mqttPublishEntry.getMsgid()))
                                .withCustomType(MQTT_PUBLISH_ENTRY_JSON_TYPE, mqttPublishEntry)
                                .build());


            } else if (message.is(ERROR_ESTIMATE_ENTRY_JSON_TYPE)) {
                ErrorEstimateEntry errorEstimateEntry = message.as(ERROR_ESTIMATE_ENTRY_JSON_TYPE);
                String msgId = errorEstimateEntry.getMsgid();
                String meta = errorEstimateEntry.getMeta();
                String analyticsType = errorEstimateEntry.getAnalyticType();
                String obsVal = errorEstimateEntry.getObsval();

                StringBuffer temp = new StringBuffer();
                String res = "";
                if (analyticsType.equals("MLR")) {
                    res = String.valueOf(errorEstimateEntry.getError());
                    temp.append(msgId)
                            .append(",")
                            .append(meta)
                            .append(",")
                            .append(analyticsType)
                            .append(",obsVal:")
                            .append(obsVal)
                            .append(",ERROR:")
                            .append(res);
                }

                if (l.isInfoEnabled()) l.info("MQTT result:{}", temp);

                HashMap<String, String> map = new HashMap<>();
                map.put(AbstractTask.DEFAULT_KEY, String.valueOf(temp));
                myKafkaProducer.doTask(map);
                MqttPublishEntry mqttPublishEntry = new MqttPublishEntry(msgId, meta, obsVal, errorEstimateEntry.getDataSetType());
                mqttPublishEntry.setArrivalTime(errorEstimateEntry.getArrivalTime());
                context.send(
                        MessageBuilder.forAddress(INBOX, String.valueOf(mqttPublishEntry.getMsgid()))
                                .withCustomType(MQTT_PUBLISH_ENTRY_JSON_TYPE, mqttPublishEntry)
                                .build());

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error in ParsePredictBeam " + e);
        }

        return context.done();
    }
}
