plugins {
  id("application")
  id("java")
  // INFO: This plugin is in `buildSrc` and manages shared dependencies.
  // id("flink-job-conventions")
}

val mainClassName = "at.ac.uibk.dps.streamprocessingapplications.TrainJob"

application { mainClass = mainClassName }

repositories {
  mavenCentral()
  maven {
    url = uri("https://packages.confluent.io/maven/") // Add Confluent repository
  }
}

dependencies {
  testImplementation(platform("org.junit:junit-bom:5.9.1"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  implementation("com.microsoft.azure:azure-storage:8.6.6")
  // https://mvnrepository.com/artifact/org.apache.beam/beam-runners-direct-java
  // runtimeOnly("org.apache.beam:beam-runners-direct-java:2.45.0")
  implementation("nz.ac.waikato.cms.weka:weka-stable:3.6.6")
  implementation("com.opencsv:opencsv:3.3")
  implementation("org.apache.commons:commons-math3:3.5")
  implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.0.2")
  implementation("org.mongodb:mongodb-driver-sync:4.3.4")
  implementation("org.apache.beam:beam-sdks-java-core:2.54.0")
  implementation("org.apache.beam:beam-runners-direct-java:2.54.0")
  implementation("org.apache.beam:beam-runners-flink-1.16:2.54.0")
  implementation("org.apache.beam:beam-sdks-java-io-kafka:2.54.0")
  // implementation("org.apache.beam:beam-sdks-java-io-mongodb:2.54.0")
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
}

java {
  sourceCompatibility = JavaVersion.VERSION_11
  targetCompatibility = JavaVersion.VERSION_11
  toolchain {
    languageVersion = JavaLanguageVersion.of(11)
    vendor = JvmVendorSpec.ADOPTOPENJDK
  }
}

tasks.named<Test>("test") { useJUnitPlatform() }

tasks.named<Jar>("jar") {
  archiveBaseName.set("TrainJob")
  destinationDirectory.set(file("build"))
  manifest {
    attributes(
        "Main-Class" to mainClassName,
    )
  }
  from("src/main/resources") { into("resources") }
  exclude("META-INF/*.SF")
  exclude("META-INF/*.DSA")
  exclude("META-INF/*.RSA")
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
  from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
  isZip64 = true
}
