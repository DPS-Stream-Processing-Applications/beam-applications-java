package at.ac.uibk.dps.streamprocessingapplications.etl.transforms;

import java.util.function.Function;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class Interpolate<T> extends PTransform<PCollection<T>, PCollection<T>> {
  TypeDescriptor<T> type;
  Function<Iterable<T>, Iterable<T>> interpolationFunction;

  Interpolate(TypeDescriptor<T> type, Function<Iterable<T>, Iterable<T>> interpolationFunction) {
    this.type = type;
    this.interpolationFunction = interpolationFunction;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return input
        .apply(WithKeys.of(""))
        .apply(GroupIntoBatches.ofSize(10))
        .apply(Values.create())
        .apply(FlatMapElements.into(type).via(this.interpolationFunction::apply));
  }
}
