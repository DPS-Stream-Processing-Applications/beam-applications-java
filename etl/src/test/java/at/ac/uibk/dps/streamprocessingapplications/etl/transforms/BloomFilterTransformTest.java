package at.ac.uibk.dps.streamprocessingapplications.etl.transforms;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.BloomFilter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Funnel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BloomFilterTransformTest {

  TestPipeline pipeline;

  @BeforeEach
  void setUp() {
    pipeline = TestPipeline.create();
  }

  @Test
  public void bloomFilterTransform_elementContained_staysInCollection() {
    TaxiRide taxiTrip = new TaxiRide();
    taxiTrip.setTaxiIdentifier("149298F6D390FA640E80B41ED31199C5");
    TestStream<TaxiRide> createEvents =
        TestStream.create(SerializableCoder.of(TaxiRide.class))
            .addElements(taxiTrip)
            .advanceWatermarkToInfinity();
    Funnel<TaxiRide> taxiToIdStringFunnel =
        (taxiRide, sink) ->
            sink.putString(taxiRide.getTaxiIdentifier().get(), StandardCharsets.UTF_8);
    BloomFilter<TaxiRide> bloomFilterTaxiRide = BloomFilter.create(taxiToIdStringFunnel, 1);
    bloomFilterTaxiRide.put(taxiTrip);

    PCollection<TaxiRide> actual =
        pipeline.apply(createEvents).apply(new BloomFilterTransform<TaxiRide>(bloomFilterTaxiRide));

    PAssert.that(actual).containsInAnyOrder(taxiTrip);
  }

  @Test
  public void bloomFilterTransform_elementNotContained_removedFromCollection() {
    TaxiRide taxiTrip = new TaxiRide();
    taxiTrip.setTaxiIdentifier("149298F6D390FA640E80B41ED31199C5");
    TestStream<TaxiRide> createEvents =
        TestStream.create(SerializableCoder.of(TaxiRide.class))
            .addElements(taxiTrip)
            .advanceWatermarkToInfinity();
    Funnel<TaxiRide> taxiToIdStringFunnel =
        (taxiRide, sink) ->
            sink.putString(taxiRide.getTaxiIdentifier().get(), StandardCharsets.UTF_8);
    BloomFilter<TaxiRide> bloomFilterTaxiRide = BloomFilter.create(taxiToIdStringFunnel, 0);

    PCollection<TaxiRide> actual =
        pipeline.apply(createEvents).apply(new BloomFilterTransform<TaxiRide>(bloomFilterTaxiRide));

    PAssert.that(actual).empty();
  }
}
