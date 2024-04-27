plugins { id("application") }

val mainClassName = "org.example.Main"

application { mainClass = mainClassName }

repositories { mavenCentral() }

dependencies {
  implementation("org.apache.kafka:kafka-clients:3.5.1")
  implementation("joda-time:joda-time:2.10.11")
  implementation("com.opencsv:opencsv:5.5.1")
  // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
  implementation("org.slf4j:slf4j-api:2.0.13")
}

java {
  sourceCompatibility = JavaVersion.VERSION_11
  targetCompatibility = JavaVersion.VERSION_11
}

tasks.named<Test>("test") { useJUnitPlatform() }

tasks.named<Jar>("jar") {
  archiveBaseName.set("KafkaProducer")
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
