package at.ac.uibk.dps.streamprocessingapplications;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import javax.annotation.Nullable;
import org.json.JSONObject;

/**
 * This class represents a SenML sensor value formed of a subset of the official specification. See:
 * https://datatracker.ietf.org/doc/html/rfc8428 The following SenML labels are required for this
 * representation:
 *
 * <ul>
 *   <li>bn
 *   <li>n
 *   <li>u
 *   <li>v
 *   <li>t
 * </ul>
 */
public class SenMLRecord implements Serializable {
    @Nullable private String baseName;

    @Nullable private String name;
    @Nullable private String unit;
    @Nullable private Float value;
    @Nullable private Instant time;

    public SenMLRecord(String senMLString) {
        JSONObject record = new JSONObject(senMLString);
        this.baseName = record.has("bn") ? record.optString("bn") : null;
        this.name = record.has("n") ? record.optString("n") : null;
        this.unit = record.has("u") ? record.optString("u") : null;
        this.value = record.has("v") ? Float.valueOf(record.optString("v")) : null;
        this.time =
                record.has("t")
                        ? Instant.ofEpochSecond(Long.parseLong(record.optString("t")))
                        : null;
    }

    public String getBaseName() {
        return baseName;
    }

    public String getName() {
        return name;
    }

    public String getUnit() {
        return unit;
    }

    public Float getValue() {
        return value;
    }

    public Instant getTime() {
        return time;
    }

    public String getFullName() {
        return this.getBaseName() + this.getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SenMLRecord)) return false;
        SenMLRecord that = (SenMLRecord) o;
        return Objects.equals(getBaseName(), that.getBaseName())
                && Objects.equals(getName(), that.getName())
                && Objects.equals(getUnit(), that.getUnit())
                && Objects.equals(getValue(), that.getValue())
                && Objects.equals(getTime(), that.getTime());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBaseName(), getName(), getUnit(), getValue(), getTime());
    }
}
