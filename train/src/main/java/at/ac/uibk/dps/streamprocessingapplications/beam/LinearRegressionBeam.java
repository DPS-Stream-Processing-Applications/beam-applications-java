package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.DbEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.TrainEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.LinearRegressionTrainBatched;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinearRegressionBeam extends DoFn<DbEntry, TrainEntry> {

    private static Logger l;
    private final String dataSetType;
    LinearRegressionTrainBatched linearRegressionTrainBatched;
    String datasetName;
    private Properties p;

    // LinearRegression lr;

    public LinearRegressionBeam(Properties p_, String datasetName) {
        this.p = p_;
        this.dataSetType = datasetName;
    }

    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Setup
    public void setup() {

        initLogger(LoggerFactory.getLogger("APP"));
        linearRegressionTrainBatched = new LinearRegressionTrainBatched();
        linearRegressionTrainBatched.setup(l, p);
    }

    @ProcessElement
    public void processElement(@Element DbEntry input, OutputReceiver<TrainEntry> out)
            throws IOException {
        String msgid = input.getMgsid();
        String trainData = input.getTrainData();
        String rowKeyEnd = input.getRowKeyEnd();

        // PrintWriter out = null;
        //        try {
        //            out = new PrintWriter("filename1.txt");
        //            out.println(trainData);
        //        } catch (FileNotFoundException e) {
        //            e.printStackTrace();
        //        }
        //        out.close();

        HashMap<String, String> map = new HashMap();
        //        obsVal="22.7,49.3,0,1955.22,27"; //dummy
        map.put(AbstractTask.DEFAULT_KEY, trainData);

        if (dataSetType.equals("SYS")) {
            datasetName = p.getProperty("TRAIN.DATASET_NAME_SYS");
        }
        if (dataSetType.equals("TAXI") | dataSetType.equals("FIT")) {
            datasetName = p.getProperty("TRAIN.DATASET_NAME_TAXI");
        }

        String filename = datasetName + "-MLR-" + rowKeyEnd + ".model";
        if (datasetName.equals("SYS")) {
            filename = datasetName + "-MLR-" + "-1422748810000" + ".model";
        }
        if (datasetName.equals("TAXI") | datasetName.equals("FIT")) {
            filename = datasetName + "-MLR-" + "1358102664000.model";
        }

        map.put("FILENAME", filename);

        Float res = linearRegressionTrainBatched.doTask(map);
        //        ByteArrayOutputStream model= (ByteArrayOutputStream)
        // linearRegressionTrainBatched.getLastResult();

        //        if(l.isInfoEnabled()) {
        //            l.info("Trained Model L.R. after bytestream object-{}", model.toString());
        ////            l.info("res linearRegressionPredictor-" + res);
        //        }

        if (res != null) {
            if (res != Float.MIN_VALUE) {
                out.output(new TrainEntry("model", msgid, rowKeyEnd, "MLR", filename));
            } else {
                if (l.isWarnEnabled()) l.warn("Error in LinearRegressionPredictorBolt");
                throw new RuntimeException();
            }
        }
    }
}
