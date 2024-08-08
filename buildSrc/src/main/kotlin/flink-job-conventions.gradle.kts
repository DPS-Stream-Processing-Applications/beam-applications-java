plugins {
    id("java")
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven")
    }
}

/* WARN:
 * The beam `KafkaIO` does not specify the kafka client which is needed to interact with the kafka topics.
 * Instead, one needs to add the `kafka-clients` dependency
 * with the version of the kafka cluster to be interfaced with.
 *
 * For this implementation it needs to match the kafka version in `kafka-cluster.yaml`.
 */
val kafkaClientsVersion = "3.7.0"

val flinkVersion = "1.18"
val beamVersion = "2.57.0"
val slf4jVersion = "1.7.32"
val log4jVersion = "2.23.1"
val junitJupiterVersion = "5.10.3"
val hamcrestVersion = "2.2"

dependencies {
    implementation(platform("org.apache.beam:beam-sdks-java-google-cloud-platform-bom:$beamVersion"))
    /* INFO:
     * The Bill Of Materials (BOM) handles the suggested versions for all the Beam dependencies.
     * No additional versions need to be specified.
     */
    implementation("org.apache.beam:beam-sdks-java-core")
    // implementation("org.apache.beam:beam-runners-direct-java")
    implementation("org.apache.beam:beam-sdks-java-io-kafka")
    implementation("org.apache.beam:beam-sdks-java-io-mongodb")
    implementation("org.apache.beam:beam-runners-flink-$flinkVersion")
    implementation("org.apache.beam:beam-runners-direct-java")

    implementation("org.slf4j:slf4j-jdk14:$slf4jVersion")
    implementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
    implementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion")

    testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
    testImplementation ("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testImplementation("org.junit.vintage:junit-vintage-engine:$junitJupiterVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation("org.hamcrest:hamcrest:$hamcrestVersion")

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

