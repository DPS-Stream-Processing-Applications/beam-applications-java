package at.ac.uibk.dps.streamprocessingapplications.beam;

import at.ac.uibk.dps.streamprocessingapplications.entity.AnnotateEntry;
import at.ac.uibk.dps.streamprocessingapplications.entity.DbEntry;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AbstractTask;
import at.ac.uibk.dps.streamprocessingapplications.tasks.AnnotateDTClass;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnnotateBeam extends DoFn<DbEntry, AnnotateEntry> {

    private static Logger l;
    AnnotateDTClass annotateDTClass;
    private Properties p;

    public AnnotateBeam(Properties p_) {
        p = p_;
    }

    public static void initLogger(Logger l_) {
        l = l_;
    }

    @Setup
    public void setup() {
        initLogger(LoggerFactory.getLogger("APP"));
        annotateDTClass = new AnnotateDTClass();
        annotateDTClass.setup(l, p);
    }

    @ProcessElement
    public void processElement(@Element DbEntry input, OutputReceiver<AnnotateEntry> out)
            throws IOException {
        String msgId = input.getMgsid();
        String data = input.getTrainData();
        String rowkeyend = input.getRowKeyEnd();

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, data);
        annotateDTClass.doTask(map);

        String annotData = annotateDTClass.getLastResult();

        AnnotateEntry entry = new AnnotateEntry(msgId, annotData, rowkeyend);
        out.output(entry);
    }
}
