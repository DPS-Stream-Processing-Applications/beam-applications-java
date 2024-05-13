package at.ac.uibk.dps.streamprocessingapplications.shared.sinks;

import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.bson.Document;

public class StoreStringInDBSink extends PTransform<PCollection<String>, PDone> {
  private String collection;

  public StoreStringInDBSink(String collection) {
    this.collection = collection;
  }

  @Override
  public PDone expand(PCollection<String> input) {
    return input
        .apply(
            MapElements.into(TypeDescriptor.of(Document.class))
                // HACK: We can not store a JSON array, so we store a document with the string as an
                // entry.
                .via(senMLString -> new Document("entry", senMLString)))
        .apply(
            MongoDbIO.write()
                .withUri(
                    "mongodb://adminuser:password123@mongodb-nodeport.default.svc.cluster.local/admin")
                .withDatabase("riot-applications")
                .withCollection(this.collection));
  }
}
