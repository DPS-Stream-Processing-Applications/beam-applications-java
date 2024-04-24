package at.ac.uibk.dps.streamprocessingapplications.etl.taxi;

public class RangeFilterFunctionTest {
  /*
  TestPipeline pipeline;

  @BeforeEach
  void setUp() {
    pipeline = TestPipeline.create();
  }

  @Test
  public void filterRange_withTripTimeInSecsLessThanOrEqual140_isSetToNull() {
    TaxiRide outOfRangeTripTime = new TaxiRide();
    outOfRangeTripTime.setTripTimeInSecs(140.0);
    TestStream<TaxiRide> createEvents =
        TestStream.create(SerializableCoder.of(TaxiRide.class))
            .addElements(outOfRangeTripTime)
            .advanceWatermarkToInfinity();

    PCollection<TaxiRide> actual = pipeline.apply(createEvents).apply(new RangeFilterFunction());

    PAssert.that(actual).containsInAnyOrder(new TaxiRide());
  }

  @Test
  public void filterRange_withTripTimeInSecs141_isUnchanged() {
    TaxiRide withinRangeTrimTime = new TaxiRide();
    withinRangeTrimTime.setTripTimeInSecs(141.0);
    TestStream<TaxiRide> createEvents =
        TestStream.create(SerializableCoder.of(TaxiRide.class))
            .addElements(withinRangeTrimTime)
            .advanceWatermarkToInfinity();

    PCollection<TaxiRide> actual = pipeline.apply(createEvents).apply(new RangeFilterFunction());

    PAssert.that(actual).containsInAnyOrder(withinRangeTrimTime);
  } */
}
