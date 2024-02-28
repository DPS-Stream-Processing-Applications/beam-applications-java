package at.ac.uibk.dps.streamprocessingapplications.etl.transforms;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FilterAndInterpolateOutliersTest {
  TestPipeline pipeline;

  @BeforeEach
  void setUp() {
    pipeline = TestPipeline.create();
  }

  @Test
  // FIX: Failing because no widow before `GroupByKey`
  void firstTest() {
    PCollection<String> filtered = pipeline.apply(OnTimeTestStream.create()).apply(new FilterAndInterpolateOutliers());
      System.out.println(filtered);
  }
}
