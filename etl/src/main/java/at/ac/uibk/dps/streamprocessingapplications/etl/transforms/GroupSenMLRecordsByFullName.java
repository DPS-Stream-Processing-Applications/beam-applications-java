package at.ac.uibk.dps.streamprocessingapplications.etl.transforms;

import at.ac.uibk.dps.streamprocessingapplications.etl.model.AbstractSenMLRecord;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class GroupSenMLRecordsByFullName<T extends AbstractSenMLRecord<?>>
    extends PTransform<PCollection<T>, PCollection<Iterable<T>>> {

  private final TypeDescriptor<T> recordType;

  public GroupSenMLRecordsByFullName(TypeDescriptor<T> recordType) {
    this.recordType = recordType;
  }

  @Override
  public PCollection<Iterable<T>> expand(PCollection<T> input) {
    return input
        .apply(
            "Map to KV(getFullName(), SenMLRecord)",
            MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), this.recordType))
                .via(senMLRecord -> KV.of(senMLRecord.getFullName(), senMLRecord)))
        // FIX: Window needs to be applied for unbounded data before `GroupByKey`
        .apply("Group SenML records by full name", GroupByKey.create())
        .apply(Values.create());
  }
}
