package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

public class KalmanFilterFunction<T> extends DoFn<KV<String, T>, KV<String, T>> {

  // INFO: Values were ported from:
  // https://github.com/dream-lab/riot-bench/blob/c86414f7f926ed5ae0fab756bb3d82fbfb6e5bf7/modules/tasks/src/main/resources/tasks_TAXI.properties
  private static final double q_processNoise = 0.125;
  private static final double r_sensorNoise = 0.32;

  @StateId("previousEstimation")
  private final StateSpec<ValueState<Double>> previousEstimationState =
      StateSpecs.value(DoubleCoder.of());

  @StateId("priorErrorCovariance")
  private final StateSpec<ValueState<Double>> priorErrorCovarianceState =
      StateSpecs.value(DoubleCoder.of());

  private SerializableFunction<T, Double> getter;
  private SerializableBiFunction<T, Double, T> setter;

  public KalmanFilterFunction(
      SerializableFunction<T, Double> getter, SerializableBiFunction<T, Double, T> setter) {
    this.getter = getter;
    this.setter = setter;
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, T> kvElement,
      OutputReceiver<KV<String, T>> out,
      @StateId("previousEstimation") ValueState<Double> previousEstimation,
      @StateId("priorErrorCovariance") ValueState<Double> priorErrorCovariance) {
    T element = kvElement.getValue();
    final double z_measuredValue = this.getter.apply(element);

    // NOTE: conditional override of the values due to `NullPointerException`
    double x0_previousEstimation = 0.0;
    try {
      x0_previousEstimation = previousEstimation.read();
    } catch (NullPointerException ignore) {
    }

    double p0_priorErrorCovariance = 0.0;
    try {
      p0_priorErrorCovariance = priorErrorCovariance.read();
    } catch (NullPointerException ignore) {
    }

    // INFO: The following code was copied from the original `riot-bench` implementation.
    double p1_currentErrorCovariance = p0_priorErrorCovariance + q_processNoise;

    double k_kalmanGain = p1_currentErrorCovariance / (p1_currentErrorCovariance + r_sensorNoise);
    double x1_currentEstimation =
        x0_previousEstimation + k_kalmanGain * (z_measuredValue - x0_previousEstimation);
    p1_currentErrorCovariance = (1 - k_kalmanGain) * p1_currentErrorCovariance;

    // Update estimate and covariance for next iteration
    previousEstimation.write(x1_currentEstimation);
    priorErrorCovariance.write(p1_currentErrorCovariance);

    out.output(KV.of("", this.setter.apply(element, x1_currentEstimation)));
  }
}
