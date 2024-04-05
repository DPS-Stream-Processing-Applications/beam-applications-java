package org.example;

public class Main {
    public static void main(String[] args) {
        String BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVER");
        String application = System.getenv("APPLICATION");
        String datasetType = System.getenv("DATASET");
        double scalingFactor = Double.parseDouble(System.getenv("SCALING"));
        String topic = System.getenv("TOPIC");

        System.out.println("Bootstrap server " + BOOTSTRAP_SERVERS);
        Producer producer = new Producer(BOOTSTRAP_SERVERS);
        EventGen eventGen = new EventGen(scalingFactor, producer, topic);

        if (application.equals("train")) {
            switch (datasetType) {
                case "SYS":
                    {
                        String csvFileName =
                                "/opt/kafkaproducer/resources/train/inputFileForTimerSpout-CITY.csv";
                        eventGen.launch(csvFileName);
                        break;
                    }
                case "FIT":
                    {
                        String csvFileName =
                                "/opt/kafkaproducer/resources/train/inputFileForTimerSpout-FIT.csv";
                        eventGen.launch(csvFileName);
                        break;
                    }
                case "TAXI":
                    {
                        String csvFileName =
                                "/opt/kafkaproducer/resources/train/inputFileForTimerSpout-TAXI.csv";
                        eventGen.launch(csvFileName);
                        break;
                    }
                default:
                    throw new RuntimeException("No valid dataset given: " + datasetType);
            }
        } else if (application.equals("pred")) {
            String csvFileName = "";
            switch (datasetType) {
                case "SYS":
                    {
                        csvFileName = "/opt/kafkaproducer/resources/pred/SYS_sample_data_senml.csv";
                        break;
                    }
                case "FIT":
                    {
                        csvFileName = "/opt/kafkaproducer/resources/pred/FIT_sample_data_senml.csv";
                        break;
                    }
                case "TAXI":
                    {
                        csvFileName =
                                "/opt/kafkaproducer/resources/pred/TAXI_sample_data_senml.csv";
                        break;
                    }
                default:
                    throw new RuntimeException("No valid dataset given: " + datasetType);
            }
            if (csvFileName.contains("senml")) {
                eventGen.launch(csvFileName, true);
            }
        }
    }
}
