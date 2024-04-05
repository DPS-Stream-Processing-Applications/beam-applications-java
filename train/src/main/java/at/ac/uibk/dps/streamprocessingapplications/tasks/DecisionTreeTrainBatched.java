package at.ac.uibk.dps.streamprocessingapplications.tasks;

import at.ac.uibk.dps.streamprocessingapplications.TrainJob;
import java.io.*;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import weka.classifiers.trees.J48;
import weka.core.Instances;

/**
 * This task should only be run from a single thread to avoid overwriting output model file.
 *
 * @author shukla, simmhan
 */
public class DecisionTreeTrainBatched extends AbstractTask {

    private static final Object SETUP_LOCK = new Object();
    // static fields common to all threads
    private static boolean doneSetup = false;

    private static String modelFilePath;
    //	private static int modelTrainFreq;
    private static String instanceHeader = null;
    private static String SAMPLE_HEADER = "";
    private static String dummyData;
    // local fields assigned to each thread
    private int instancesCount = 0;
    private StringBuffer instancesBuf = null;

    /***
     *
     * @param instancesReader
     * @param modelFilePath
     * @param model
     *@param l  @return
     */
    private static int decisionTreeTrainAndSaveModel(
            StringReader instancesReader,
            String modelFilePath,
            ByteArrayOutputStream model,
            Logger l) {

        Instances trainingData = WekaUtil.loadDatasetInstances(instancesReader, l);

        if (trainingData == null) return -1;

        try {
            // train the model
            J48 j48tree = new J48();
            j48tree.buildClassifier(trainingData);
            if (l.isInfoEnabled()) l.info("Model is - " + j48tree.toString());

            //			System.out.println(("Model is - "+j48tree.toString()));

            // saving the model
            // weka.core.SerializationHelper.write(modelFilePath, j48tree);

        } catch (Exception e) {
            l.warn("error training decision tree", e);
            System.out.println(e);
            return -2;
        }

        if (l.isInfoEnabled()) {
            l.info("Decision tree Model trained and saved at {} ", modelFilePath);
            l.info("number of training instances {} ", trainingData.numInstances());
        }

        return trainingData.numInstances(); // return number of instances trained on
    }

    public void setup(Logger l_, Properties p_) {
        // TODO: Later, have option of training using instances present in data file rather than
        // just from messages
        super.setup(l_, p_);
        synchronized (SETUP_LOCK) {
            if (!doneSetup) { // Do setup only once for this task
                modelFilePath = p_.getProperty("TRAIN.DECISION_TREE.MODEL_PATH");
                //				modelTrainFreq=
                // Integer.parseInt(p_.getProperty("PREDICT.DECISION_TREE.TRAIN.MODEL_UPDATE_FREQUENCY"));
                // converting arff file with header only to string
                //					instanceHeader = WekaUtil.readFileToString(arffFilePath,
                // StandardCharsets.UTF_8);
                instanceHeader = p_.getProperty("CLASSIFICATION.DECISION_TREE.SAMPLE_HEADER");
                instanceHeader = "/resources/model/DecisionTreeClassify-SYS.arff";

                doneSetup = true;
            }
        }
        // setup for NON-static fields
        instancesCount = 0;

        StringBuffer stringBuffer;
        try (InputStream inputStream =
                TrainJob.class.getResourceAsStream(
                        "/resources/model/DecisionTreeClassify-SYS.arff")) {
            if (inputStream == null) {
                throw new RuntimeException(
                        "Resource not found: /resources/model/DecisionTreeClassify-SYS.arff");
            }
            // Read the content of the resource
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            stringBuffer = new StringBuffer();
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuffer.append(line).append("\n");
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        instancesBuf = (stringBuffer);

        //		try {
        //			dummyData=new
        // String(readAllBytes(Paths.get(p_.getProperty("CLASSIFICATION.DECISION_TREE.DUMMY_DATA"))));
        //		} catch (IOException e) {
        //			e.printStackTrace();
        //		}
    }

    @Override
    protected Float doTaskLogic(Map map) throws IOException {

        //		m="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
        //		String modelname="TEST-DTC.model"+ UUID.randomUUID();
        //		map.put("FILENAME",modelname);

        String m = (String) map.get(AbstractTask.DEFAULT_KEY);
        String filename = (String) map.get("FILENAME");
        // String arffContent = new String(Files.readAllBytes(Paths.get(instanceHeader)));
        // instancesBuf = new StringBuffer(arffContent);
        ByteArrayOutputStream model = new ByteArrayOutputStream();
        if (l.isInfoEnabled()) l.info("Range query res:{}", m);
        System.out.println(modelFilePath);
        String fullFilePath = filename; //  model file updated with MLR-endRowkey.model
        int result = 0;
        try {

            // instancesBuf.append("\n").append(m).append("\n");
            // train and save model
            l.info("instancesBuf-" + instancesBuf.toString());
            StringReader stringReader = new StringReader(instancesBuf.toString());
            result = decisionTreeTrainAndSaveModel(stringReader, fullFilePath, model, l);

            /*
            if(l.isInfoEnabled()) {
                l.info("Trained Model L.R.-{}", weka.core.SerializationHelper.read(fullFilePath).toString());
            }
             */

            super.setLastResult(model);
            if (result >= 0) return Float.valueOf(0); // success

        } catch (Exception e) {
            l.warn("error training decision tree" + e);
            throw new RuntimeException(e);
        }

        return Float.valueOf(Float.MIN_VALUE);
    }
}