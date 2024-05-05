package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.AnnotateDTClass;
import org.apache.flink.statefun.playground.java.greeter.types.AnnotateEntry;
import org.apache.flink.statefun.playground.java.greeter.types.DbEntry;
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

import static org.apache.flink.statefun.playground.java.greeter.types.Types.ANNOTATE_ENTRY_JSON_TYPE;
import static org.apache.flink.statefun.playground.java.greeter.types.Types.Db_ENTRY_JSON_TYPE;


public class AnnotateFn implements StatefulFunction {
    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/annotation");

    static final TypeName INBOX = TypeName.typeNameFromString("pred/decisionTreeTrain");

    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(AnnotateFn::new)
                    .build();

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
        annotateDTClass = new AnnotateDTClass();
        annotateDTClass.setup(l, p);
    }


    private static Logger l;
    AnnotateDTClass annotateDTClass;
    private Properties p;

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        setup();
        DbEntry dbEntry = message.as(Db_ENTRY_JSON_TYPE);
        String msgId = dbEntry.getMgsid();
        String data = dbEntry.getTrainData();
        String rowkeyend = dbEntry.getRowKeyEnd();

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, data);
        annotateDTClass.doTask(map);

        String annotData = annotateDTClass.getLastResult();

        AnnotateEntry entry = new AnnotateEntry(msgId, annotData, rowkeyend, dbEntry.getDataSetType());

        context.send(
                MessageBuilder.forAddress(INBOX, String.valueOf(entry.getMsgid()))
                        .withCustomType(ANNOTATE_ENTRY_JSON_TYPE, entry)
                        .build());
        return context.done();
    }
}
