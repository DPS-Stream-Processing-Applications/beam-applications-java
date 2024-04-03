package at.ac.uibk.dps.streamprocessingapplications.etl.taxi.transforms;

import at.ac.uibk.dps.streamprocessingapplications.etl.model.SenMLRecordDouble;
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.model.TaxiRide;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BloomFilterTransformTest {

  TestPipeline pipeline;

  @BeforeEach
  void setUp() {
    pipeline = TestPipeline.create();
  }

  @Test
  public void bloomFilter() {
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
    // INFO: setting explicitly throws `NullPointerException`
    // outOfRangeTripTime.setTripTimeInSecs(140.0);
    TestStream<TaxiRide> createEvents =
        TestStream.create(SerializableCoder.of(TaxiRide.class))
            .addElements(outOfRangeTripTime)
            .advanceWatermarkToInfinity();

    PCollection<TaxiRide> actual = pipeline.apply(createEvents).apply(new BloomFilterTransform());
  }
}
