package org.apache.flink.statefun.playground.java.greeter.tasks;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
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
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;

/**
 * This task is thread-safe, and can be run from multiple threads.
 *
 * @author shukla, simmhan
 */
public class LinearRegressionPredictor extends AbstractTask<String, Float> {

    private static final Object SETUP_LOCK = new Object();

    private static final Object DATABASE_LOCK = new Object();
    // for taxi dataset
    private static final String SAMPLE_INPUT = "420,1.95,8.00";
    public static LinearRegression lr;
    // static fields common to all threads
    private static volatile boolean doneSetup = false;
    // private static final String SAMPLE_INPUT = "-71.10,42.37,10.1,65.3,0";
    private static int useMsgField;

    // private static String SAMPLE_HEADER = "";
    //			"@RELATION city_data\n" +
    //			"\n" +
    ////			"@ATTRIBUTE Longi            NUMERIC\n" +
    ////			"@ATTRIBUTE Lat              NUMERIC\n" +
    //			"@ATTRIBUTE Temp             NUMERIC\n" +
    //			"@ATTRIBUTE Humid            NUMERIC\n" +
    //			"@ATTRIBUTE Light            NUMERIC\n" +
    //			"@ATTRIBUTE Dust             NUMERIC\n" +
    //			"@ATTRIBUTE airquality           NUMERIC\n" +
    //			"\n" +
    //			"@DATA\n" +
    //			"%header format";
    private static String modelFilePath;
    private static String SAMPLE_HEADER =
            "@RELATION taxi_data\n"
                    + "\n"
                    + "@ATTRIBUTE triptimeInSecs             NUMERIC\n"
                    + "@ATTRIBUTE tripDistance            NUMERIC\n"
                    + "@ATTRIBUTE fareAmount           NUMERIC\n"
                    + "\n"
                    + "@DATA\n"
                    + "%header format";
    private static Instances instanceHeader;

    private String databaseUrl;

    private String databaseName;

    public LinearRegressionPredictor(String databaseUrl, String databaseName) {
        this.databaseUrl = databaseUrl;
        this.databaseName = databaseName;
    }

    public void setup(Logger l_, Properties p_) {
        super.setup(l_, p_);
        if (doneSetup) {
            System.out.println("doneSetup is True");
        }
        if (!doneSetup) {
            synchronized (this) {
                if (!doneSetup) {
                    try {
                        useMsgField = Integer.parseInt(p_.getProperty("PREDICT.LINEAR_REGRESSION.USE_MSG_FIELD", "0"));
                        modelFilePath = "LR-TAXI-Numeric_model";
                        byte[] pdfData = null;

                        MongoClientSettings settings = MongoClientSettings.builder()
                                .applyConnectionString(new ConnectionString(databaseUrl))
                                .build();
                        try (MongoClient mongoClient = MongoClients.create(settings)) {
                            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
                            MongoCollection<Document> collection = mongoDatabase.getCollection("pdfCollection");

                            Document query = new Document(modelFilePath, new Document("$exists", true));
                            Document projection = new Document(modelFilePath, 1).append("_id", 0);

                            for (Document document : collection.find(query).projection(projection)) {
                                Binary pdfBinary = (Binary) document.get(modelFilePath);
                                pdfData = pdfBinary.getData();
                            }
                            if (pdfData == null) {
                                throw new RuntimeException("null content in db");
                            }
                        } catch (MongoException e) {
                            System.err.println(e);
                        }

                        // Model deserialization
                        byte[] csvContent = pdfData;
                        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent);
                             ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
                            lr = (LinearRegression) objectInputStream.readObject();

                            if (lr == null) {
                                throw new RuntimeException("lr is null");
                            }
                            if (l.isInfoEnabled()) l.info("Model is {} ", lr.toString());

                            instanceHeader = WekaUtil.loadDatasetInstances(new StringReader(SAMPLE_HEADER), l);
                            if (l.isInfoEnabled()) l.info("Header is {}", instanceHeader);
                            if (instanceHeader == null) {
                                throw new RuntimeException("instanceHeader is null");
                            }

                            doneSetup = true;
                        } catch (Exception e) {
                            l.warn("error loading decision tree model from file: " + modelFilePath, e);
                            doneSetup = false;
                            throw new RuntimeException("Error loading decision tree model from file", e);
                        }
                    } catch (Exception e) {
                        l.error("Error in read from db", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    @Override
    protected Float doTaskLogic(Map<String, String> map) {
        String m = map.get(AbstractTask.DEFAULT_KEY);
        Instance testInstance = null;

        try {
            String[] testTuple;
            if (useMsgField > 0) {
                testTuple = m.split(",");
            } else {
                testTuple = SAMPLE_INPUT.split(",");
            }
            //			testTuple="22.7,49.3,0,1955.22,27".split(","); //dummy

            instanceHeader = WekaUtil.loadDatasetInstances(new StringReader(SAMPLE_HEADER), l);
            if (instanceHeader == null) {
                throw new RuntimeException("InstanceHeader is null");
            }

            testInstance = WekaUtil.prepareInstance(instanceHeader, testTuple, l);

            if (instanceHeader == null) {
                throw new RuntimeException("instanceHeader is null");
            }

            if (lr == null) {
                throw new RuntimeException("lr is null");

            }
            int prediction = (int) lr.classifyInstance(testInstance);
            //int prediction = 1;
            if (l.isInfoEnabled()) {
                l.info(" ----------------------------------------- ");
                l.info("Test data               : {}", testInstance);
                l.info("Test data prediction result {}", prediction);
            }

            return super.setLastResult((float) prediction);

        } catch (Exception e) {
            l.warn("error with classification of testInstance: " + testInstance, e);
            //throw new RuntimeException("Exception in doTaskLogic of Linear_Reg_Pred " + e);
            return super.setLastResult(Float.MIN_VALUE);
        }
    }
}
