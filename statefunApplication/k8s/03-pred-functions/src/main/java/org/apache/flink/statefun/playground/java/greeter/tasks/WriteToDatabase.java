package org.apache.flink.statefun.playground.java.greeter.tasks;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
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

      byte[] pdfData = readFileToByteArray(path);

      Document document = new Document();
      document.append(key, pdfData);
      collection.insertOne(document);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void prepareDataBaseForApplication() {
    saveFileIntoDb(
        "/resources/DecisionTreeClassify-SYS.model",
        "DecisionTreeClassify-SYS_model");
    saveFileIntoDb(
        "/resources/DecisionTreeClassify-SYS-withExcellent.model",
        "DecisionTreeClassify-SYS-withExcellent_model");
    saveFileIntoDb(
        "/resources/DecisionTreeClassify-TAXI-withVeryGood.model",
        "DecisionTreeClassify-TAXI-withVeryGood_model");
    saveFileIntoDb(
        "/resources/FIT_sample_data_senml.csv",
        "FIT_sample_data_senml_csv");
    saveFileIntoDb(
        "/resources/LR-TAXI-Numeric.model", "LR-TAXI-Numeric_model");
    saveFileIntoDb("/resources/mhealth_schema.csv", "mhealth_schema_csv");
    saveFileIntoDb(
        "/resources/sys-schema_without_annotationfields.txt",
        "sys-schema_without_annotationfields_txt");
    saveFileIntoDb(
        "/resources/SYS_sample_data_senml.csv",
        "SYS_sample_data_senml_csv");
    saveFileIntoDb(
        "/resources/SYS_sample_data_senml_small.csv",
        "SYS_sample_data_senml_small_csv");
    saveFileIntoDb(
        "/resources/taxi-schema-without-annotation.csv",
        "taxi-schema-without-annotation_csv");
    saveFileIntoDb(
        "/resources/TAXI_sample_data_senml.csv",
        "TAXI_sample_data_senml_csv");
    saveFileIntoDb("/resources/test.arff", "test_arff");
  }
}