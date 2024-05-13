package at.ac.uibk.dps.streamprocessingapplications.database;

import at.ac.uibk.dps.streamprocessingapplications.PredJob;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.*;
import org.bson.Document;

public class WriteToDatabase implements Serializable {

  private String databaseUrl;

  private String dataBaseName;

  public WriteToDatabase(String databaseUrl, String dataBaseName) {
    this.databaseUrl = databaseUrl;
    this.dataBaseName = dataBaseName;
  }

  private static byte[] readFileToByteArray(String filePath) throws IOException {
    File file = new File(filePath);
    byte[] fileData = new byte[(int) file.length()];
    try (FileInputStream fis = new FileInputStream(file)) {
      fis.read(fileData);
    }
    return fileData;
  }

  public void saveFileIntoDb(String path, String key) {
    try (MongoClient mongoClient = MongoClients.create(databaseUrl)) {
      MongoDatabase database = mongoClient.getDatabase(dataBaseName);

      MongoCollection<Document> collection = database.getCollection("pdfCollection");

      // byte[] pdfData = readFileToByteArray(path);
      byte[] pdfData;

      try (InputStream inputStream = PredJob.class.getResourceAsStream(path)) {
        pdfData = inputStream.readAllBytes();
      } catch (Exception e) {
        throw new RuntimeException("Exception when trying to save files into db");
      }

      if (pdfData == null) {
        throw new RuntimeException("Content to be saved to db is empty");
      }

      Document document = new Document();
      document.append(key, pdfData);
      collection.insertOne(document);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void prepareDataBaseForApplication() {
    saveFileIntoDb("/datasets/DecisionTreeClassify-SYS.model", "DecisionTreeClassify-SYS_model");
    saveFileIntoDb(
        "/datasets/DecisionTreeClassify-SYS-withExcellent.model",
        "DecisionTreeClassify-SYS-withExcellent_model");
    saveFileIntoDb(
        "/datasets/DecisionTreeClassify-TAXI-withVeryGood.model",
        "DecisionTreeClassify-TAXI-withVeryGood_model");
    saveFileIntoDb("/datasets/FIT_sample_data_senml.csv", "FIT_sample_data_senml_csv");
    saveFileIntoDb("/datasets/LR-TAXI-Numeric.model", "LR-TAXI-Numeric_model");
    saveFileIntoDb("/datasets/mhealth_schema.csv", "mhealth_schema_csv");
    saveFileIntoDb(
        "/datasets/sys-schema_without_annotationfields.txt",
        "sys-schema_without_annotationfields_txt");
    saveFileIntoDb("/datasets/SYS_sample_data_senml.csv", "SYS_sample_data_senml_csv");
    saveFileIntoDb("/datasets/SYS_sample_data_senml_small.csv", "SYS_sample_data_senml_small_csv");
    saveFileIntoDb(
        "/datasets/taxi-schema-without-annotation.csv", "taxi-schema-without-annotation_csv");
    saveFileIntoDb("/datasets/TAXI_sample_data_senml.csv", "TAXI_sample_data_senml_csv");
    saveFileIntoDb("/datasets/test.arff", "test_arff");
  }
}
