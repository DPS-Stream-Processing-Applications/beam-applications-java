FROM quay.io/strimzi/kafka:0.40.0-kafka-3.7.0

# Copy your application JAR or binary into the image
COPY build/KafkaProducer.jar /opt/kafkaproducer/

# Set the working directory
WORKDIR /opt/kafkaproducer/


COPY src/main/resources/pred /opt/kafkaproducer/resources/pred
COPY src/main/resources/train /opt/kafkaproducer/resources/train


# Command to run your application
CMD ["java", "-jar", "KafkaProducer.jar"]
