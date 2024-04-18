package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.math3.stat.regression.SimpleRegression;

public class SlidingLinearRegression<T> extends DoFn<KV<String, T>, KV<String, List<Double>>> {
  private final int trainWindowSize;
  private final int predictionHorizonSize;

  @StateId("previousElements")
  private final StateSpec<BagState<Double>> previousElements = StateSpecs.bag();

  private SerializableFunction<T, Double> getter;

  public SlidingLinearRegression(
      SerializableFunction<T, Double> getter, int trainWindowSize, int predictionHorizonSize) {
    this.getter = getter;
    this.trainWindowSize = trainWindowSize;
    this.predictionHorizonSize = predictionHorizonSize;
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, T> kvElement,
      OutputReceiver<KV<String, List<Double>>> out,
      @StateId("previousElements") BagState<Double> previousElements) {

    Double newValue = this.getter.apply(kvElement.getValue());
    List<Double> pastElements =
        StreamSupport.stream(previousElements.read().spliterator(), false)
            .collect(Collectors.toList());

    // Update `pastElements`
    pastElements.add(newValue);
    if (pastElements.size() > this.trainWindowSize) {
      pastElements.remove(0);
    }

    // Populate regression
    SimpleRegression regression = new SimpleRegression();
    // NOTE: The regression needs pairs of x and y values.
    // The original implementation uses a global counter, but I think it can also be done with
    // relative counters.
    for (int i = 0; i < pastElements.size(); i++) {
      regression.addData(i, pastElements.get(i));
    }

    // Predict `predictionHorizonSize` many elements
    List<Double> predictions = new ArrayList<>();
    for (int i = 0; i < this.predictionHorizonSize; i++) {
      predictions.add(regression.predict(pastElements.size() + i));
    }

    out.output(KV.of("", predictions));
  }
}
