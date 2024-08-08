package at.ac.uibk.dps.streamprocessingapplications.tasks;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.codec.binary.Base64;
import org.bson.Document;
import org.slf4j.Logger;
import weka.classifiers.functions.LinearRegression;
import weka.core.Instances;
import weka.core.SerializationHelper;

/**
 * This task should only be run from a single thread to avoid overwriting output model file.
 *
 * @author shukla, simmhan
 */
public class LinearRegressionTrainBatched extends AbstractTask<String, ByteArrayOutputStream> {

  private static final Object SETUP_LOCK = new Object();
  private static boolean doneSetup = false;

  private static String modelFilePath;
  private static String instanceHeader = null;

  private static String dataSetType;
  private static String HEADER_TAXI =
      "@RELATION taxi_data\n"
          + "\n"
          + "@ATTRIBUTE triptimeInSecs             NUMERIC\n"
          + "@ATTRIBUTE tripDistance            NUMERIC\n"
          + "@ATTRIBUTE fareAmount           NUMERIC\n"
          + "\n"
          + "@DATA\n"
          + "%header format";

  private static String HEADER_SYS =
      "@RELATION city_data\n"
          + "\n"
          + "@ATTRIBUTE Longi            NUMERIC\n"
          + "@ATTRIBUTE Lat              NUMERIC\n"
          + "@ATTRIBUTE Temp             NUMERIC\n"
          + "@ATTRIBUTE Humid            NUMERIC\n"
          + "@ATTRIBUTE Light            NUMERIC\n"
          + "@ATTRIBUTE Dust             NUMERIC\n"
          + "\n"
          + "@DATA\n"
          + "%header format";

  private static String HEADER_FIT =
      "@RELATION FIT_data\n"
          + "\n"
          + " @attribute subjectId numeric@attribute acc_chest_x numeric@attribute acc_chest_y"
          + " numeric @attribute acc_chest_z numeric @attribute ecg_lead_1 numeric @attribute"
          + " ecg_lead_2 numeric  @attribute acc_ankle_x numeric  @attribute acc_ankle_y numeric "
          + " @attribute acc_ankle_z numeric  @attribute gyro_ankle_x numeric @attribute"
          + " gyro_ankle_y numeric @attribute gyro_ankle_z numeric  @attribute magnetometer_ankle_x"
          + " numeric @attribute magnetometer_ankle_y numeric @attribute magnetometer_ankle_z"
          + " numeric @attribute acc_arm_x numeric  @attribute acc_arm_y numeric @attribute"
          + " acc_arm_z numeric  @attribute gyro_arm_x numeric @attribute gyro_arm_y numeric"
          + " @attribute gyro_arm_z numeric @attribute magnetometer_arm_x numeric @attribute"
          + " magnetometer_arm_y numeric @attribute magnetometer_arm_z numeric  @attribute label"
          + " {label_value_1, label_value_2, label_value_3}  % Assuming label is nominal\n"
          + "@DATA\n"
          + "%header format";

  private StringBuffer instancesBuf = null;

  private String dataBaseUrl;

  public LinearRegressionTrainBatched(String databaseUrl) {
    this.dataBaseUrl = databaseUrl;
  }

  /**
   * @param instanceReader
   * @param modelFilePath
   * @param model
   * @param l @return
   */
  private int linearRegressionTrainAndSaveModel(
      StringReader instanceReader, String modelFilePath, ByteArrayOutputStream model, Logger l) {

    Instances trainingData = WekaUtil.loadDatasetInstances(instanceReader, l);
    if (trainingData == null) {
      throw new RuntimeException("trainings-data in linearRegressionTrain is null");
    }

    try {
      // train the model
      LinearRegression lr = new LinearRegression();
      lr.buildClassifier(trainingData);

      /*
      weka.core.SerializationHelper.write(model, lr);

      // saving the model
      weka.core.SerializationHelper.write(modelFilePath, lr);

       */
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      SerializationHelper.write(byteArrayOutputStream, lr);
      byte[] modelBytes = byteArrayOutputStream.toByteArray();
      String modelBase64 = Base64.encodeBase64String(modelBytes);
      MongoClient mongoClient = MongoClients.create(dataBaseUrl);
      MongoDatabase database = mongoClient.getDatabase("mydb");
      MongoCollection<Document> collection = database.getCollection("pdfCollection");
      Document modelDocument = new Document();
      modelDocument.append("model_test_2", modelBytes);
      modelDocument.append("createdAt", new Date());
      collection.insertOne(modelDocument);

      mongoClient.close();

    } catch (Exception e) {
      l.warn("error training linear regression", e);
      throw new RuntimeException("error training linear regression: " + e);
    }
    if (l.isInfoEnabled())
      l.info("linear regression Model trained over full ARFF file and saved at {} ", modelFilePath);
    return 0;
  }

  public void setup(Logger l_, Properties p_, String dataSetType) {
    super.setup(l_, p_);
    synchronized (SETUP_LOCK) {
      if (!doneSetup) { // Do setup only once for this task
        modelFilePath = p_.getProperty("TRAIN.LINEAR_REGRESSION.MODEL_PATH");
        //				String arffFilePath =
        // p_.getProperty("PREDICT.LINEAR_REGRESSION.TRAIN.ARFF_PATH");
        //				modelTrainFreq=
        // Integer.parseInt(p_.getProperty("PREDICT.LINEAR_REGRESSION.TRAIN.MODEL_UPDATE_FREQUENCY"));

        // converting arff file with header only to string
        //					instanceHeader = WekaUtil.readFileToString(arffFilePath,
        // StandardCharsets.UTF_8);
        if (dataSetType.equals("TAXI")) {
          instanceHeader = HEADER_TAXI;
        } else if (dataSetType.equals("FIT")) {
          instanceHeader = HEADER_FIT;
        } else if (dataSetType.equals("SYS")) {
          instanceHeader = HEADER_SYS;
        }
        doneSetup = true;
      }
    }
    // setup for NON-static fields
    instancesBuf = new StringBuffer(instanceHeader);
    //		try {
    //			dummyData=new
    // String(readAllBytes(Paths.get(p_.getProperty("PREDICT.LINEAR_REGRESSION.DUMMY_DATA"))));
    //		} catch (IOException e) {
    //			e.printStackTrace();
    //		}

  }

  @Override
  protected Float doTaskLogic(Map<String, String> map) {

    //		m="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
    //		System.out.println("instancesBuf-"+instancesBuf.toString());
    // code for micro benchmark : START
    //		dummyData=""
    //		String modelname="TEST-MLR.model"+ UUID.randomUUID();
    //		map.put("FILENAME",modelname);
    //		map.put(AbstractTask.DEFAULT_KEY,dummyData);
    //		// code for micro benchmark : END

    String trainingsData = map.get(AbstractTask.DEFAULT_KEY);
    String filename = map.get("FILENAME");
    ByteArrayOutputStream model = new ByteArrayOutputStream();

    String fullFilePath = modelFilePath + filename;
    int result;
    try {

      instancesBuf.append("\n").append(trainingsData).append("\n");
      if (l.isInfoEnabled()) l.info("instancesBuf-" + instancesBuf.toString());
      StringReader stringReader = new StringReader(instancesBuf.toString());
      result = linearRegressionTrainAndSaveModel(stringReader, fullFilePath, model, l);
      if (result != 0) {
        throw new RuntimeException("Error when training and saving a model");
      }

      if (l.isInfoEnabled()) {
        LinearRegression readModel =
            (LinearRegression) weka.core.SerializationHelper.read(fullFilePath);
        l.info("Trained Model L.R.-{}", readModel.toString());
      }

      super.setLastResult(model);
      instancesBuf = new StringBuffer(instanceHeader);
    } catch (Exception e) {
      l.warn("error training decision tree", e);
    }
    return 0f;
  }
}
