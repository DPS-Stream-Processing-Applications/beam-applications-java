package at.ac.uibk.dps.streamprocessingapplications.database;

import at.ac.uibk.dps.streamprocessingapplications.FlinkJob;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.*;
import java.util.Date;
import org.bson.Document;

public class WriteToDatabase implements Serializable {

  private final String databaseUrl;

  private final String dataBaseName;

  public WriteToDatabase(String databaseUrl, String dataBaseName) {
    this.databaseUrl = databaseUrl;
    this.dataBaseName = dataBaseName;
  }

  public void saveFileIntoDb(String path, String key) {
    try (var mongoClient = MongoClients.create(databaseUrl)) {
      MongoDatabase database = mongoClient.getDatabase(dataBaseName);
      MongoCollection<Document> collection = database.getCollection("pdfCollection");

      // byte[] pdfData = readFileToByteArray(path);
      byte[] pdfData;

      try (InputStream inputStream = FlinkJob.class.getResourceAsStream(path)) {
          assert inputStream != null;
          pdfData = inputStream.readAllBytes();
      } catch (Exception e) {
        throw new RuntimeException("Exception when trying to save files into db");
      }

      if (pdfData == null) {
        throw new RuntimeException("Content to be saved to db is empty");
      }

      Document document = new Document();
      document.append(key, pdfData);
      document.append("createdAt", new Date());
      collection.insertOne(document);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void prepareDataBaseForApplication() {
    saveFileIntoDb("/model/DecisionTreeClassify-SYS.arff", "DecisionTreeClassify-SYS_arff");
  }
}
