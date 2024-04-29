plugins {
    id("java")
    /* NOTE:
     * Using the Java `toolchain` a specific java version and implementation can be specified.
     * the following resolver allows gradle to install this java version if it is not available on the build machine.
     */
    // id("org.gradle.toolchains.foojay-resolver-convention")
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven")
    }
}

dependencies {
    implementation("org.apache.beam:beam-sdks-java-core:2.54.0")
    implementation("org.apache.beam:beam-runners-direct-java:2.54.0")
    implementation("org.apache.beam:beam-runners-flink-1.16:2.54.0")
    implementation("org.apache.beam:beam-sdks-java-io-kafka:2.54.0")
    implementation("org.slf4j:slf4j-jdk14:1.7.32")
    implementation("org.apache.logging.log4j:log4j-core:2.23.1")
    /* INFO:
     * The beam `KafkaIO` does not specify the kafka client which is needed to interact with the kafka topics.
     * Instead, one needs to add the `kafka-clients` dependency
     * with the version of the kafka cluster to be interfaced with.
     *
     * For this implementation it needs to match the kafka version in `kafka-cluster.yaml`.
     */
    implementation("org.apache.kafka:kafka-clients:3.7.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.hamcrest:hamcrest:2.2")
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
        vendor = JvmVendorSpec.ADOPTOPENJDK
    }

}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

