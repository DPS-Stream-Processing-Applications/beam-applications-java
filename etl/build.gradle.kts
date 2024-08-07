plugins {
  id("java")
  // INFO: This plugin is in `buildSrc` and manages shared dependencies.
  id("flink-job-conventions")
  id ("com.github.johnrengelman.shadow") version "7.1.2"
}

repositories { mavenCentral() }

dependencies {
  implementation("org.json:json:20240205")
  implementation(project(":shared"))
  implementation("commons-cli:commons-cli:1.8.0")
}

tasks.shadowJar {
  setProperty("zip64", true)
  mergeServiceFiles()
}

tasks.named<Jar>("jar") {
  archiveBaseName.set("FlinkJob")
  destinationDirectory.set(file("build"))
  manifest {
    /* NOTE:
     * Allows the mainClass can be overridden using `-PmainClass=<custom_main_class>`
     */
    val mainClass =
        project.findProperty("mainClass")?.toString()
            ?: "at.ac.uibk.dps.streamprocessingapplications.etl.FlinkJob"
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
