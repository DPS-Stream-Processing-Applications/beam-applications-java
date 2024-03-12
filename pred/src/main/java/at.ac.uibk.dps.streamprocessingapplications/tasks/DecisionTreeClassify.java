package at.ac.uibk.dps.streamprocessingapplications.tasks;

import java.io.StringReader;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;

/**
 * This task is thread-safe, and can be run from multiple threads.
 *
 * @author shukla, simmhan
 */
public class DecisionTreeClassify extends AbstractTask {

    private static final Object SETUP_LOCK = new Object();
    // static fields common to all threads
    private static boolean doneSetup = false;
    private static int useMsgField;
    private final String dataSetType;

    // Sample data, assuming arff file has headers for Sense-Your-City dataset
    private static final String SAMPLE_INPUT_SYS = "-71.10,42.37,10.1,65.3,0";
    // for taxi dataset
    private static final String SAMPLE_INPUT_TAXI = "420,1.95,8.00";
    //	// Encode the arff header for SYS as a constant string

    private static final String SAMPLE_HEADER_SYS =
            "@RELATION SYS_data\n"
                    + "\n"
                    + "@ATTRIBUTE Longi            NUMERIC\n"
                    + "@ATTRIBUTE Lat              NUMERIC\n"
                    + "@ATTRIBUTE Temp             NUMERIC\n"
                    + "@ATTRIBUTE Humid            NUMERIC\n"
                    + "@ATTRIBUTE Light            NUMERIC\n"
                    + "@ATTRIBUTE Dust             NUMERIC\n"
                    + "@ATTRIBUTE airquality       NUMERIC\n"
                    + "@ATTRIBUTE result           {Bad,Average,Good,VeryGood,Excellent}\n"
                    + "\n"
                    + "@DATA\n"
                    + "%header format";

    //	// Sample data, assuming arff file has headers for TAXI dataset

    //	// Encode the arff header for SYS as a constant string
    private static final String SAMPLE_HEADER_TAXI =
            "@RELATION TAXI_data\n"
                    + "\n"
                    + "@ATTRIBUTE triptimeInSecs            NUMERIC\n"
                    + "@ATTRIBUTE tripDistance             NUMERIC\n"
                    + "@ATTRIBUTE fareAmount       NUMERIC\n"
                    + "@ATTRIBUTE result           {Bad,Good,VeryGood}\n"
                    + "\n"
                    + "@DATA\n"
                    + "%header format";

    private static Instances instanceHeader;
    private static int resultAttrNdx; // Index of result attribute in arff file
    public static J48 j48tree;

    public void setup(Logger l_, Properties p_) {
        super.setup(l_, p_);
        synchronized (SETUP_LOCK) {
            if (!doneSetup) { // Do setup only once for this task
                // If positive, use actual tuple as input else SAMPLE_INPUT
                String modelFilePath = "";
                String sampleHeader = "";
                try {

                    if (dataSetType.equals("SYS")) {
                        useMsgField =
                                Integer.parseInt(
                                        p_.getProperty(
                                                "CLASSIFICATION.DECISION_TREE.USE_MSG_FIELD", "0"));
                        modelFilePath =
                                p_.getProperty("CLASSIFICATION.DECISION_TREE.MODEL_PATH_SYS");
                        // attribute index for getting the resulting enum
                        resultAttrNdx =
                                Integer.parseInt(
                                        p_.getProperty(
                                                "CLASSIFICATION.DECISION_TREE.CLASSIFY.RESULT_ATTRIBUTE_INDEX_SYS"));
                        sampleHeader = SAMPLE_HEADER_SYS;
                    }
                    if (dataSetType.equals("TAXI")) {
                        useMsgField =
                                Integer.parseInt(
                                        p_.getProperty(
                                                "CLASSIFICATION.DECISION_TREE.USE_MSG_FIELD", "0"));
                        modelFilePath =
                                p_.getProperty("CLASSIFICATION.DECISION_TREE.MODEL_PATH_TAXI");
                        // attribute index for getting the resulting enum
                        resultAttrNdx =
                                Integer.parseInt(
                                        p_.getProperty(
                                                "CLASSIFICATION.DECISION_TREE.CLASSIFY.RESULT_ATTRIBUTE_INDEX_TAXI"));
                        sampleHeader = SAMPLE_HEADER_TAXI;
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Exception when setting up decisionTree " + e);
                }
                try {
                    j48tree = (J48) weka.core.SerializationHelper.read(modelFilePath);
                    if (l.isInfoEnabled()) l.info("Model is {}", j48tree);

                    // SAMPLE_HEADER=p_.getProperty("CLASSIFICATION.DECISION_TREE.SAMPLE_HEADER");
                    instanceHeader =
                            WekaUtil.loadDatasetInstances(new StringReader(sampleHeader), l);
                    if (l.isInfoEnabled()) l.info("Header is {}", instanceHeader);

                    doneSetup = true;
                } catch (Exception e) {
                    l.warn("error loading decision tree model from file: " + modelFilePath, e);
                    doneSetup = false;
                    throw new RuntimeException("Exception, when loading decision tree model " + e);
                }
            }
        }
    }

    @Override
    protected Float doTaskLogic(Map map) {
        String m = (String) map.get(AbstractTask.DEFAULT_KEY);
        Instance testInstance = null;
        try {
            String[] testTuple = null;
            if (useMsgField > 0) { // useMsgField is used as flag
                testTuple = m.split(",");
            } else {
                //				System.out.println("TestS : in do task" );
                if (dataSetType.equals("SYS")) {
                    testTuple = SAMPLE_INPUT_SYS.split(",");
                }
                if (dataSetType.equals("TAXI")) {
                    testTuple = SAMPLE_INPUT_TAXI.split(",");
                }
            }
            if (dataSetType.equals("TAXI")) {
                instanceHeader =
                        WekaUtil.loadDatasetInstances(new StringReader(SAMPLE_HEADER_TAXI), l);
            }
            if (dataSetType.equals("SYS")) {
                instanceHeader =
                        WekaUtil.loadDatasetInstances(new StringReader(SAMPLE_HEADER_SYS), l);
            }
            testInstance = WekaUtil.prepareInstance(instanceHeader, testTuple, l);


            //FIXME: Fault is in the classifyInstance
            int classification = (int) j48tree.classifyInstance(testInstance);
            //int classification = 2;
            System.out.println("DT result from task  " + classification);
            // String result = instanceHeader.attribute(resultAttrNdx - 1).value(classification);

            // System.out.println("DT result from task  " + result);
            if (l.isInfoEnabled()) {
                l.info(" ----------------------------------------- ");
                l.info("Test data               : {}", testInstance);
                // l.info("Test data classification result {}, {}", result, classification);
            }
            return Float.valueOf(classification);
        } catch (Exception e) {
            l.warn("error with classification of testInstance: " + testInstance, e);
            throw new RuntimeException(e);
            // return Float.valueOf(Float.MIN_VALUE);
        }
    }

    public DecisionTreeClassify(String dataSetType) {
        this.dataSetType = dataSetType;
    }
}
