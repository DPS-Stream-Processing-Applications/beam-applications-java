package at.ac.uibk.dps.streamprocessingapplications.etl.transforms;

import at.ac.uibk.dps.streamprocessingapplications.etl.SenMLParser;
import at.ac.uibk.dps.streamprocessingapplications.etl.model.SenMLRecordDouble;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FilterAndInterpolateOutliers
    extends PTransform<PCollection<String>, PCollection<String>> {
  @Override
  public PCollection<String> expand(PCollection<String> input) {
    return input
        .apply(
            "Parse SenMLRecord POJO",
            MapElements.into(TypeDescriptor.of(SenMLRecordDouble.class))
                .via(SenMLParser::parseJsonStringWithV))
        .apply(
            "Group records by full name",
            new GroupSenMLRecordsByFullName<SenMLRecordDouble>(
                TypeDescriptor.of(SenMLRecordDouble.class)))
        // .apply(
        // "Batch into count of 5",
        // Window.<Iterable<SenMLRecord>>into(new GlobalWindows())
        // .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(5)))
        // .discardingFiredPanes())
        .apply(
            "Get timestamp",
            MapElements.into(TypeDescriptors.iterables(TypeDescriptor.of(Instant.class)))
                .via(
                    records ->
                        StreamSupport.stream(records.spliterator(), true)
                            .map(SenMLRecordDouble::getTime)
                            .collect(Collectors.toList())))
        .apply(ToString.elements());
  }
}
