package org.apache.flink.statefun.playground.java.greeter.tasks;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.Binary;
import org.slf4j.Logger;
import weka.classifiers.functions.LinearRegression;
import weka.core.Instance;
import weka.core.Instances;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * This task is thread-safe, and can be run from multiple threads.
 *
 * @author shukla, simmhan
 */
public class LinearRegressionPredictor extends AbstractTask<String, Float> {

    // for taxi dataset
    private final String SAMPLE_INPUT = "420,1.95,8.00";
    private LinearRegression lr;
    // static fields common to all threads
    private boolean doneSetup = false;
    // private static final String SAMPLE_INPUT = "-71.10,42.37,10.1,65.3,0";
    private int useMsgField;
    private String SAMPLE_HEADER =
            "@RELATION taxi_data\n"
                    + "\n"
                    + "@ATTRIBUTE triptimeInSecs             NUMERIC\n"
                    + "@ATTRIBUTE tripDistance            NUMERIC\n"
                    + "@ATTRIBUTE fareAmount           NUMERIC\n"
                    + "\n"
                    + "@DATA\n"
                    + "%header format";

    private String databaseUrl;

    private String databaseName;

    public LinearRegressionPredictor(String databaseUrl, String databaseName) {
        this.databaseUrl = databaseUrl;
        this.databaseName = databaseName;
    }

    public void setup(Logger l_, Properties p_) {
        super.setup(l_, p_);
    }

    public void setLr(LinearRegression lr) {
        this.lr = lr;
    }

    @Override
    protected Float doTaskLogic(Map<String, String> map) {
        // Asynchronous DB access to fetch model
        CompletableFuture<LinearRegression> futureModel = fetchModelFromMongoDB();

        // When model is retrieved, make predictions
        return futureModel.thenApply(lr -> {
            if (lr == null) {
                l.warn("Linear regression model is null");
                return super.setLastResult(Float.MIN_VALUE);
            }

            String m = map.get(AbstractTask.DEFAULT_KEY);
            Instance testInstance = null;
            String[] testTuple;
            Instances instanceHeader = null;
            int prediction;
            if (useMsgField > 0) {
                testTuple = m.split(",");
            } else {
                testTuple = SAMPLE_INPUT.split(",");
            }
            //testTuple="22.7,49.3,0,1955.22,27".split(",");
            try {
                WekaUtil wekaUtil = new WekaUtil();
                instanceHeader = wekaUtil.loadDatasetInstances(new StringReader(SAMPLE_HEADER), l);
                testInstance = wekaUtil.prepareInstance(instanceHeader, testTuple, l);
            } catch (Exception e) {
                l.warn("Error preparing test instance", e);
                return super.setLastResult(Float.MIN_VALUE);
            }

            try {
                prediction = (int) lr.classifyInstance(testInstance);
            } catch (Exception e) {
                l.warn("Error during model prediction", e);
                return super.setLastResult(Float.MIN_VALUE);
            }

            return super.setLastResult((float) prediction);
        }).exceptionally(ex -> {
            l.error("Error fetching model or making prediction", ex);
            return super.setLastResult(Float.MIN_VALUE);
        }).join();
    }


    private CompletableFuture<LinearRegression> fetchModelFromMongoDB() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                MongoClientSettings settings = MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(databaseUrl))
                        .build();
                try (MongoClient mongoClient = MongoClients.create(settings)) {
                    MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
                    MongoCollection<Document> collection = mongoDatabase.getCollection("pdfCollection");

                    String modelFilePath = "LR-TAXI-Numeric_model";
                    Document query = new Document(modelFilePath, new Document("$exists", true));
                    Document projection = new Document(modelFilePath, 1).append("_id", 0);

                    Binary pdfBinary = null;
                    for (Document document : collection.find(query).projection(projection)) {
                        pdfBinary = (Binary) document.get(modelFilePath);
                    }

                    if (pdfBinary == null) {
                        throw new RuntimeException("Model data is null in MongoDB");
                    }

                    // Deserialize the model
                    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(pdfBinary.getData());
                         ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
                        return (LinearRegression) objectInputStream.readObject();
                    }
                }
            } catch (RuntimeException | ClassNotFoundException | IOException e) {
                throw new RuntimeException("Error fetching or deserializing model from MongoDB", e);
            }
        });
    }
}