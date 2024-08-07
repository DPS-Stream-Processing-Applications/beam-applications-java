plugins {
  id("java")
  // INFO: This plugin is in `buildSrc` and manages shared dependencies.
  id("flink-job-conventions")
  id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories {
  mavenCentral()
  maven {
    url = uri("https://packages.confluent.io/maven/") // Add Confluent repository
  }
}

dependencies {
  implementation("org.json:json:20240205")
  implementation("com.microsoft.azure:azure-storage:4.0.0")
  implementation("nz.ac.waikato.cms.weka:weka-stable:3.6.6")
  implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.0.2")
  implementation("com.opencsv:opencsv:3.3")
  implementation("com.googlecode.json-simple:json-simple:1.1")
  implementation("org.mongodb:mongodb-driver-sync:4.3.4")
}

tasks {
  // INFO: The `shadowJar` task replaces the default jar.
  jar {
    enabled = false
    archiveBaseName.set("FlinkJob")
    destinationDirectory.set(file("build"))
    // INFO: This manifest configuration gets inherited by the `shadowJar` task.
    manifest {
      val mainClass = "at.ac.uibk.dps.streamprocessingapplications.FlinkJob"
      attributes(
          "Main-Class" to mainClass,
      )
    }
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    isZip64 = true
  }

  shadowJar {
    archiveBaseName = jar.get().archiveBaseName
    archiveClassifier = jar.get().archiveClassifier
    destinationDirectory = jar.get().destinationDirectory
    setProperty("zip64", jar.get().isZip64)
    mergeServiceFiles()
  }

  build { dependsOn(shadowJar) }
}
