package at.ac.uibk.dps.streamprocessingapplications.etl.model;

import javax.annotation.Nullable;
import java.time.Instant;

public class SenMLRecordString  extends  AbstractSenMLRecord<String>{
    public SenMLRecordString(@Nullable String baseName, @Nullable String name, @Nullable String unit, @Nullable String value, @Nullable Instant time) {
        super(baseName, name, unit, value, time);
    }
}
