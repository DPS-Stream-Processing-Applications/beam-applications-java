package at.ac.uibk.dps.streamprocessingapplications.tasks;

import at.ac.uibk.dps.streamprocessingapplications.PredJob;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import weka.classifiers.functions.LinearRegression;
import weka.core.Instance;
import weka.core.Instances;

/**
 * This task is thread-safe, and can be run from multiple threads.
 *
 * @author shukla, simmhan
 */
public class LinearRegressionPredictor extends AbstractTask<String, Float> {

    private static final Object SETUP_LOCK = new Object();
    // for taxi dataset
    private static final String SAMPLE_INPUT = "420,1.95,8.00";
    public static LinearRegression lr;
    // static fields common to all threads
    private static boolean doneSetup = false;
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

    /**
     * @param l_
     * @param p_
     */
    public void setup(Logger l_, Properties p_) {
        super.setup(l_, p_);
        synchronized (SETUP_LOCK) {
            if (!doneSetup) { // Do setup only once for this task
                // If positive use actual input for prediction else use
                // dummyInputConst
                useMsgField =
                        Integer.parseInt(
                                p_.getProperty("PREDICT.LINEAR_REGRESSION.USE_MSG_FIELD", "0"));

                // modelFilePath = p_.getProperty("PREDICT.LINEAR_REGRESSION.MODEL_PATH");
                modelFilePath = "/resources/datasets/LR-TAXI-Numeric.model";

                if (modelFilePath == null) {
                    throw new RuntimeException("modelFilePath null");
                }

                try (InputStream inputStream = PredJob.class.getResourceAsStream(modelFilePath);
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {

                    if (inputStream == null) {
                        throw new RuntimeException("Model file not found: " + modelFilePath);
                    }

                    StringBuilder modelContent = new StringBuilder();
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        modelContent.append(line).append("\n");
                    }

                    /*
                    lr =
                            (LinearRegression)
                                    weka.core.SerializationHelper.read(
                                            new ObjectInputStream(inputStream2));

                     */
                    lr = null;
                    // if (l.isInfoEnabled()) l.info("Model is {} ", lr.toString());

                    /*
                    if (lr == null) {
                        throw new RuntimeException("lr null");
                    }

                     */

                    // SAMPLE_HEADER = p_.getProperty("PREDICT.LINEAR_REGRESSION.SAMPLE_HEADER");

                    if (SAMPLE_HEADER == null) {
                        throw new RuntimeException("sampleheader null");
                    }

                    instanceHeader =
                            WekaUtil.loadDatasetInstances(new StringReader(SAMPLE_HEADER), l);
                    if (l.isInfoEnabled()) l.info("Header is {}", instanceHeader);
                    if (instanceHeader == null) {
                        throw new RuntimeException("instanceHeader is null");
                    }

                    doneSetup = true;
                } catch (Exception e) {
                    l.warn("error loading decision tree model from file: " + modelFilePath, e);
                    doneSetup = false;
                    throw new RuntimeException("Error loading decision tree model from file " + e);
                }
            }
        }
    }

    @Override
    protected Float doTaskLogic(Map map) {
        String m = (String) map.get(AbstractTask.DEFAULT_KEY);
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

            // int prediction = (int) lr.classifyInstance(testInstance);
            int prediction = 1;
            if (l.isInfoEnabled()) {
                l.info(" ----------------------------------------- ");
                l.info("Test data               : {}", testInstance);
                l.info("Test data prediction result {}", prediction);
            }

            // set parent to have the actual predictions
            return super.setLastResult((float) prediction);

        } catch (Exception e) {
            l.warn("error with classification of testInstance: " + testInstance, e);
            // set parent to have the actual predictions
            throw new RuntimeException("Exception in doTaskLogic of Linear_Reg_Pred " + e);
            // return super.setLastResult(Float.MIN_VALUE);
        }
    }
}
