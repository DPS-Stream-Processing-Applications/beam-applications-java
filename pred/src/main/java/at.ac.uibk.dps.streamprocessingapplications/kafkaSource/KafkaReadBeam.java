package at.ac.uibk.dps.streamprocessingapplications.kafkaSource;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.jetbrains.annotations.NotNull;

public class KafkaReadBeam extends PTransform<PBegin, PCollection<String>> {

    private final String kafkaBootstrapServers;
    private final String kafkaTopic;

    private final long numberOfLines;

    public KafkaReadBeam(String kafkaBootstrapServers, String kafkaTopic, long numberOfLines) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
        this.numberOfLines = numberOfLines;
    }

    @NotNull @Override
    public PCollection<String> expand(PBegin input) {
        return input.apply(
                "Read from Kafka",
                Read.from(
                        new KafkaConsumerSource(kafkaBootstrapServers, kafkaTopic, numberOfLines)));
    }
}
