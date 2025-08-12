plugins {
  id("java")
  // INFO: This plugin is in `buildSrc` and manages shared dependencies.
  id("flink-job-conventions")
  id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories { mavenCentral() }

dependencies {
  implementation("org.json:json:20240205")
  implementation(project(":shared"))
  implementation("commons-cli:commons-cli:1.8.0")
  implementation(project(":etl"))
  implementation(project(":stats"))
  implementation(project(":train"))
  implementation(project(":pred"))
}

tasks {
  // Ensure compileJava waits for etl/pred shaded jars
  named("compileJava") {
    dependsOn(":etl:shadowJar", ":stats:shadowJar", ":train:shadowJar", ":pred:shadowJar")
  }

  // INFO: The `shadowJar` task replaces the default jar.
  jar {
    enabled = false
    archiveBaseName.set("FlinkJob")
    destinationDirectory.set(file("build"))
    // INFO: This manifest configuration gets inherited by the `shadowJar` task.
    manifest {
      /* NOTE:
       * Allows the mainClass to be overridden using `-PmainClass=<custom_main_class>`
       */
      val mainClass =
          project.findProperty("mainClass")?.toString()
              ?: "at.ac.uibk.dps.streamprocessingapplications.riotbenchsinglejob.FlinkJob"
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
