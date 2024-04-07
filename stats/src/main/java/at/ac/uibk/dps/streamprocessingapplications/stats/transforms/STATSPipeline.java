package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class STATSPipeline<T> extends PTransform<PCollection<String>, PCollection<T>> {
  private final TypeDescriptor<T> typeDescriptor;
  private final SerializableFunction<String, T> parser;

  public STATSPipeline(TypeDescriptor<T> typeDescriptor, SerializableFunction<String, T> parser) {
    this.typeDescriptor = typeDescriptor;
    this.parser = parser;
  }

  @Override
  public PCollection<T> expand(PCollection<String> input) {
    return input.apply("Parse", MapElements.into(this.typeDescriptor).via(this.parser));
  }
}
