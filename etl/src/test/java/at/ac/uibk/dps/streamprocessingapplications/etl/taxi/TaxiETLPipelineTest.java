package at.ac.uibk.dps.streamprocessingapplications.etl.taxi;

import at.ac.uibk.dps.streamprocessingapplications.etl.transforms.ETLPipeline;
import at.ac.uibk.dps.streamprocessingapplications.shared.TaxiSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaxiETLPipelineTest {
  Pipeline testPipeline;
  PCollection<String> input;

  @BeforeEach
  void setUp() {
    this.testPipeline = TestPipeline.create();
    this.input = testPipeline.apply(TaxiTestObjects.buildSimpleTestStream());
  }

  @Test
  void integrationTest() {
    PCollection<TaxiRide> actual =
        this.input.apply(
            new ETLPipeline<>(
                TypeDescriptor.of(TaxiRide.class),
                TaxiSenMLParserJSON::parseSenMLPack,
                new RangeFilterFunction(),
                TaxiTestObjects.buildTestBloomFilter(),
                new InterpolationFunction(),
                5,
                new AnnotationFunction()));

    actual
        .apply(MapElements.into(TypeDescriptors.strings()).via(TaxiRide::toString))
        .apply(ParDo.of(new TaxiETLPipelineTest.PrintFn()));
  }

  static class PrintFn extends DoFn<String, Void> {
    @ProcessElement
    public void processElement(@Element String element) {
      System.out.println(element);
    }
  }
}
