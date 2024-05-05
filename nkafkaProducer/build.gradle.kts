plugins {
  id("java")
  id("application")
}

repositories { mavenCentral() }

dependencies {
  implementation("com.opencsv:opencsv:5.9")
  /* INFO:
   * For this implementation it needs to match the kafka version in `kafka-cluster.yaml`.
   */
  implementation("org.apache.kafka:kafka-clients:3.7.0")

  testImplementation(platform("org.junit:junit-bom:5.9.1"))
  testImplementation("org.junit.jupiter:junit-jupiter")
}

val mainClassName = "at.ac.uibk.dps.streamprocessingapplications.eventGenerators.KafkaProducer.Main"

java {
  sourceCompatibility = JavaVersion.VERSION_11
  targetCompatibility = JavaVersion.VERSION_11
  toolchain {
    languageVersion = JavaLanguageVersion.of(11)
    vendor = JvmVendorSpec.ADOPTOPENJDK
  }
}

application { mainClass = mainClassName }

tasks.test { useJUnitPlatform() }

tasks.named<Jar>("jar") {
  archiveBaseName.set("KafkaProducer")
  destinationDirectory.set(file("build"))
  manifest {
    attributes(
        "Main-Class" to mainClassName,
    )
  }
  exclude("META-INF/*.SF")
  exclude("META-INF/*.DSA")
  exclude("META-INF/*.RSA")
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
  from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
  isZip64 = true
}

/*tasks.named<CreateStartScripts>("CreateStartScripts") {
     val mainClassName = "at.ac.uibk.dps.streamprocessingapplications.eventGenerator.KafkaProducer"
     mainClass = mainClassName
     applicationName = "kafkaProducer"
     outputDir = file("build")
     classpath = files("build/KafkaProducer.jar")
 }*/
