package at.ac.uibk.dps.streamprocessingapplications.shared.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

public class AddTimestamp<T> extends PTransform<PCollection<T>, PCollection<TimestampedValue<T>>> {

  @Override
  public PCollection<TimestampedValue<T>> expand(PCollection<T> input) {
    return input.apply(
        ParDo.of(
            new DoFn<T, TimestampedValue<T>>() {
              @ProcessElement
              public void processElement(
                  @Element T element, OutputReceiver<TimestampedValue<T>> out) {
                out.output(TimestampedValue.of(element, Instant.now()));
              }
            }));
  }
}
