package org.apache.flink.statefun.playground.java.greeter.tasks;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.Map;
import org.bson.Document;
import org.bson.types.Binary;

public class ReadFromDatabaseTask extends AbstractTask<String, byte[]> {

  private final String connectionUrl;
  private final String databaseName;

  public ReadFromDatabaseTask(String connectionUrl, String databaseName) {
    this.connectionUrl = connectionUrl;
    this.databaseName = databaseName;
  }

  @Override
  protected Float doTaskLogic(Map<String, String> map) {
    String fileName = map.get("fileName");

    try (MongoClient mongoClient = MongoClients.create(connectionUrl)) {
      MongoDatabase database = mongoClient.getDatabase(databaseName);
      MongoCollection<Document> collection = database.getCollection("pdfCollection");

      Document query = new Document(fileName, new Document("$exists", true));

      Document projection = new Document(fileName, 1).append("_id", 0);

      byte[] pdfData = null;
      for (Document document : collection.find(query).projection(projection)) {
        Binary pdfBinary = (Binary) document.get(fileName);
        pdfData = pdfBinary.getData();
      }

      super.setLastResult(pdfData);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return 1f;
  }
}
