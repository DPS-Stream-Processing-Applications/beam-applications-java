package at.ac.uibk.dps.streamprocessingapplications.etl.taxi.transforms;

import at.ac.uibk.dps.streamprocessingapplications.etl.model.SenMLRecordDouble;
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.model.TaxiRide;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FilterRangeTest {
  TestPipeline pipeline;

  @BeforeEach
  void setUp() {
    pipeline = TestPipeline.create();
  }

  @Test
  public void filterRange_withTripTimeInSecsLessThanOrEqual140_isSetToNull() {
    TaxiRide outOfRangeTripTime =
        new TaxiRide(
            null,
            null,
            null,
            new SenMLRecordDouble("", "", "", 140.0, null),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    // BUG: setting explicitly throws `NullPointerException`
    // outOfRangeTripTime.setTripTimeInSecs(140.0);
    TestStream<TaxiRide> createEvents =
        TestStream.create(SerializableCoder.of(TaxiRide.class))
            .addElements(outOfRangeTripTime)
            .advanceWatermarkToInfinity();

    PCollection<TaxiRide> actual = pipeline.apply(createEvents).apply(new RangeFilter());

    PAssert.that(actual).containsInAnyOrder(new TaxiRide());
  }

  @Test
  public void filterRange_withTripTimeInSecs141_isUnchanged() {
    TaxiRide withinRangeTrimTime =
        new TaxiRide(
            null,
            null,
            null,
            new SenMLRecordDouble("", "", "", 141.0, null),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    // BUG: setting explicitly throws `NullPointerException`
    // outOfRangeTripTime.setTripTimeInSecs(140.0);
    TestStream<TaxiRide> createEvents =
        TestStream.create(SerializableCoder.of(TaxiRide.class))
            .addElements(withinRangeTrimTime)
            .advanceWatermarkToInfinity();

    PCollection<TaxiRide> actual = pipeline.apply(createEvents).apply(new RangeFilter());

    PAssert.that(actual).containsInAnyOrder(withinRangeTrimTime);
  }
}
