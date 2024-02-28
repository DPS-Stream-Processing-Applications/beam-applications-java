package at.ac.uibk.dps.streamprocessingapplications.etl.transforms;

import at.ac.uibk.dps.streamprocessingapplications.etl.model.SenMLRecord;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class GroupSenMLRecordsByFullName
    extends PTransform<PCollection<SenMLRecord>, PCollection<Iterable<SenMLRecord>>> {

  @Override
  public PCollection<Iterable<SenMLRecord>> expand(PCollection<SenMLRecord> input) {
    return input
        .apply(
            "Map to KV(getFullName(), SenMLRecord)",
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(), TypeDescriptor.of(SenMLRecord.class)))
                .via(senMLRecord -> KV.of(senMLRecord.getFullName(), senMLRecord)))
        .apply("Group SenML records by full name", GroupByKey.create())
        .apply(Values.create());
  }
}
