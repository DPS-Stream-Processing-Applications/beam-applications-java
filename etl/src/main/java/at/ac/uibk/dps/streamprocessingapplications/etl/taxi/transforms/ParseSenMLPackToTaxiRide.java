package at.ac.uibk.dps.streamprocessingapplications.etl.taxi.transforms;

import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.TaxiSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.model.TaxiRide;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class ParseSenMLPackToTaxiRide
    extends PTransform<PCollection<String>, PCollection<TaxiRide>> {
  @Override
  public PCollection<TaxiRide> expand(PCollection<String> input) {
    return input.apply(
        "Transform JSON SenML pack to `TaxiRide` POJO",
        MapElements.into(TypeDescriptor.of(TaxiRide.class))
            .via(TaxiSenMLParserJSON::parseSenMLPack));
  }
}
