package at.ac.uibk.dps.streamprocessingapplications.tasks;

import at.ac.uibk.dps.streamprocessingapplications.FlinkJob;
import java.io.InputStream;
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
public class DecisionTreeClassify extends AbstractTask<String, String> {

  private static final Object SETUP_LOCK = new Object();
  // Sample data, assuming arff file has headers for Sense-Your-City dataset
  private static final String SAMPLE_INPUT_SYS = "-71.10,42.37,10.1,65.3,0";
  // for taxi dataset
  private static final String SAMPLE_INPUT_TAXI = "420,1.95,8.00";
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
  public static J48 j48tree;
  //	// Encode the arff header for SYS as a constant string
  // static fields common to all threads
  private static boolean doneSetup = false;

  //	// Sample data, assuming arff file has headers for TAXI dataset
  private static int useMsgField;
  private static Instances instanceHeader;
  private static int resultAttrNdx;
  private final String dataSetType;

  public DecisionTreeClassify(String dataSetType) {
    this.dataSetType = dataSetType;
  }

  public void setup(Logger l_, Properties p_) {
    super.setup(l_, p_);
    synchronized (SETUP_LOCK) {
      if (!doneSetup) { // Do setup only once for this task
        // If positive, use actual tuple as input else SAMPLE_INPUT
        String modelFilePath = "";
        String sampleHeader = "";
        try {

          if (dataSetType.equals("SYS") | dataSetType.equals("FIT")) {
            useMsgField =
                Integer.parseInt(p_.getProperty("CLASSIFICATION.DECISION_TREE.USE_MSG_FIELD", "0"));
            modelFilePath = p_.getProperty("CLASSIFICATION.DECISION_TREE.MODEL_PATH_SYS");
            // attribute index for getting the resulting enum
            resultAttrNdx =
                Integer.parseInt(
                    p_.getProperty(
                        "CLASSIFICATION.DECISION_TREE.CLASSIFY.RESULT_ATTRIBUTE_INDEX_SYS"));
            sampleHeader = SAMPLE_HEADER_SYS;
          }
          if (dataSetType.equals("TAXI")) {
            useMsgField =
                Integer.parseInt(p_.getProperty("CLASSIFICATION.DECISION_TREE.USE_MSG_FIELD", "0"));
            modelFilePath = p_.getProperty("CLASSIFICATION.DECISION_TREE.MODEL_PATH_TAXI");
            // attribute index for getting the resulting enum
            resultAttrNdx =
                Integer.parseInt(
                    p_.getProperty(
                        "CLASSIFICATION.DECISION_TREE.CLASSIFY.RESULT_ATTRIBUTE_INDEX_TAXI"));
            sampleHeader = SAMPLE_HEADER_TAXI;
          }
          if (dataSetType.equals("FIT")) {
            useMsgField = -1;
          }
        } catch (Exception e) {
          throw new RuntimeException("Exception when setting up decisionTree " + e);
        }
        try {
          if (dataSetType.equals("TAXI")) {
            InputStream inputStream =
                FlinkJob.class.getResourceAsStream(
                    "/resources/datasets/DecisionTreeClassify-TAXI-withVeryGood.model");
            j48tree = (J48) weka.core.SerializationHelper.read(inputStream);
            instanceHeader = WekaUtil.loadDatasetInstances(new StringReader(sampleHeader), l);
          }
          if (dataSetType.equals("SYS") | dataSetType.equals("FIT")) {
            InputStream inputStream =
                FlinkJob.class.getResourceAsStream(
                    "/resources/datasets/DecisionTreeClassify-SYS-withExcellent.model");
            j48tree = (J48) weka.core.SerializationHelper.read(inputStream);
            instanceHeader = WekaUtil.loadDatasetInstances(new StringReader(sampleHeader), l);
          }

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
  protected Float doTaskLogic(Map<String, String> map) {
    String m = map.get(AbstractTask.DEFAULT_KEY);
    Instance testInstance = null;
    try {
      String[] testTuple = null;
      boolean isCityOrFit = dataSetType.equals("SYS") | dataSetType.equals("FIT");
      if (useMsgField > 0) {
        testTuple = m.split(",");
      } else {
        if (isCityOrFit) {
          testTuple = SAMPLE_INPUT_SYS.split(",");
        }
        if (dataSetType.equals("TAXI")) {
          testTuple = SAMPLE_INPUT_TAXI.split(",");
        }
      }
      if (dataSetType.equals("TAXI")) {
        instanceHeader = WekaUtil.loadDatasetInstances(new StringReader(SAMPLE_HEADER_TAXI), l);
      }
      if (isCityOrFit) {
        instanceHeader = WekaUtil.loadDatasetInstances(new StringReader(SAMPLE_HEADER_SYS), l);
      }
      testInstance = WekaUtil.prepareInstance(instanceHeader, testTuple, l);
      l.debug("test {}", testInstance);
      System.out.println("Instance: " + testInstance);
      l.warn("test{}", testInstance);

      if (j48tree == null) {
        throw new RuntimeException("tree is null");
      }

      int classification = (int) j48tree.classifyInstance(testInstance);
      return (float) classification;
    } catch (Exception e) {
      l.warn("error with classification of testInstance: " + testInstance, e);
      throw new RuntimeException(e);
    }
  }
}
