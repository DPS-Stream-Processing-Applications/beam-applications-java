package at.ac.uibk.dps.streamprocessingapplications.tasks;

import com.mongodb.MongoInterruptedException;
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

  private final Object databaseLock = new Object();

  public ReadFromDatabaseTask(String connectionUrl, String databaseName) {
    this.connectionUrl = connectionUrl;
    this.databaseName = databaseName;
  }

  @Override
  protected Float doTaskLogic(Map<String, String> map) {
    String fileName = map.get("fileName");
    synchronized (databaseLock) {
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
      } catch (MongoInterruptedException e) {
        l.warn("MongoInterupted Exception");
        throw new RuntimeException("MongoDbInterupt");
        // return 1F;

      } catch (Exception e) {
        e.printStackTrace();
        l.warn(e + " in DB-Task");
        throw new RuntimeException("DB: " + e);
      }
    }
    return 1f;
  }
}
