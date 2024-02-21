package at.ac.uibk.dps.streamprocessingapplications;

import org.json.JSONObject;
import org.json.JSONString;

import java.time.Instant;

/**
 * This class represents a SenML sensor value formed of a subset of the official specification.
 * See: https://datatracker.ietf.org/doc/html/rfc8428
 * The following SenML labels are required for this representation:
 * <ul>
 *     <li>bn</li>
 *     <li>n</li>
 *     <li>u</li>
 *     <li>v</li>
 *     <li>t</li>
 * </ul>
 */
public class SenMLRecord {
    private String baseName;

    private String name;
    private String unit;
    private Float value;
    private Instant time;

    public SenMLRecord(String senMLString) {
        JSONObject record = new JSONObject(senMLString);
        this.baseName = record.has("bn") ? record.optString("bn") : null;
        this.name = record.has("n") ? record.optString("n") : null;
        this.unit = record.has("u") ? record.optString("u") : null;
        this.value = record.has("v") ? Float.valueOf(record.optString("v")) : null;
        this.time = record.has("t") ? Instant.ofEpochSecond(Long.parseLong(record.optString("t")))  : null;
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

}

