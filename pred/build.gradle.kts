plugins {
  id("application")
  id("java")
  // INFO: This plugin is in `buildSrc` and manages shared dependencies.
  // id("flink-job-conventions")
}

val mainClassName = "at.ac.uibk.dps.streamprocessingapplications.FlinkJob"

application { mainClass = mainClassName }

repositories {
  mavenCentral()
  maven {
    url = uri("https://packages.confluent.io/maven/") // Add Confluent repository
  }
}

dependencies {
  implementation("org.apache.beam:beam-sdks-java-core:2.54.0")
  implementation("org.apache.beam:beam-runners-direct-java:2.54.0")
  implementation("org.apache.beam:beam-runners-flink-1.16:2.54.0")
  implementation("org.apache.beam:beam-sdks-java-io-kafka:2.54.0")
  implementation("org.slf4j:slf4j-jdk14:1.7.32")
  implementation("ch.qos.logback:logback-classic:1.3.11")
  implementation("org.apache.logging.log4j:log4j-core:2.23.1")
  implementation("org.json:json:20240205")
  implementation("com.microsoft.azure:azure-storage:4.0.0")
  implementation("nz.ac.waikato.cms.weka:weka-stable:3.6.6")
  implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.0.2")
  implementation("com.opencsv:opencsv:3.3")
  implementation("com.googlecode.json-simple:json-simple:1.1")
  implementation("org.apache.kafka:kafka-clients:3.7.0")
  // implementation("org.apache.kafka:kafka-clients:3.0.0")
  // https://mvnrepository.com/artifact/org.apache.beam/beam-sdks-java-io-kafka
  // implementation("org.apache.beam:beam-sdks-java-io-kafka:2.55.0")
  implementation("org.mongodb:mongodb-driver-sync:4.3.4")
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
  archiveBaseName.set("FlinkJob")
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
  from("src/main/resources/logback.xml")
  from("\"src/main/resources/log4j2.xml")
  isZip64 = true
}
