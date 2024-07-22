package org.apache.flink.statefun.playground.java.greeter.tasks;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;

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
                "/resources/DecisionTreeClassify-SYS.arff",
                "DecisionTreeClassify-SYS_arff");
    }
}