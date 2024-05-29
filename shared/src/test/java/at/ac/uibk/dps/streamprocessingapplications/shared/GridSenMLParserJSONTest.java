package at.ac.uibk.dps.streamprocessingapplications.shared;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.GridMeasurement;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import org.junit.jupiter.api.Test;

public class GridSenMLParserJSONTest {
    @Test
    void parseSenMLPack_allExpectedRecords() {
        String test =
                "[{\"n\":  \"id\",  \"u\":  \"double\",  \"v\":  19503.0},{\"n\":  \"grid_measurement\",  \"u\":  \"double\",  \"v\":  0.14},{\"n\":  \"timestamp\",  \"u\":  \"s\",  \"v\":  1392.0}]";
                GridMeasurement measurement = GridSenMLParserJSON.parseSenMLPack(test);
        System.out.println(measurement);
    }
}
