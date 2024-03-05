package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.AnnotateEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.TrainEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.DecisionTreeTrainBatched;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecisionTreeBeam extends DoFn<AnnotateEntry, TrainEntry> {
    private static Logger l;
    private Properties p;

    DecisionTreeTrainBatched decisionTreeTrainBatched;
    String datasetName = "";

    public DecisionTreeBeam(Properties p_) {
        this.p = p_;
    }

    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Setup
    public void setup() throws IOException {
        initLogger(LoggerFactory.getLogger("APP"));
        datasetName = p.getProperty("TRAIN.DATASET_NAME").toString();

        decisionTreeTrainBatched = new DecisionTreeTrainBatched();

        decisionTreeTrainBatched.setup(l, p);
    }

    @ProcessElement
    public void processElement(@Element AnnotateEntry input, OutputReceiver<TrainEntry> out)
            throws IOException {
        String msgid = input.getMsgid();
        String annotData = input.getAnnotData();
        String rowKeyEnd = input.getRowKeyEnd();

        //        PrintWriter out = null;
        //        try {
        //            out = new PrintWriter("filename-DTC-sample.txt");
        //            out.println(annotData);
        //        } catch (FileNotFoundException e) {
        //            e.printStackTrace();
        //        }
        //        out.close();

        //        String obsVal="22.7,49.3,0,1955.22,27"; //dummy
        //        String msgId="0";

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, annotData);
        String filename = datasetName + "-DTC-" + rowKeyEnd + ".model";
        filename = "/DecisionTreeClassify-SYS.model";
        map.put("FILENAME", filename);

        Float res = decisionTreeTrainBatched.doTask(map); // index of result-class/enum as return
        //        ByteArrayOutputStream model= (ByteArrayOutputStream)
        // decisionTreeTrainBatched.getLastResult();

        if (l.isInfoEnabled()) l.info("result from res:{}", res);

        if (res != null) {
            if (res != Float.MIN_VALUE) {
                out.output(new TrainEntry("model", msgid, rowKeyEnd, "MLR", filename));

            } else {
                if (l.isWarnEnabled()) l.warn("Error in DecisionTreeClassifyBeam");
                throw new RuntimeException("Error in DecisionTreeClassifyBeam " + res);
            }
        }
    }
}