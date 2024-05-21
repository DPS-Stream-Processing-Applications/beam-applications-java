plugins {
  id("java")
  // INFO: This plugin is in `buildSrc` and manages shared dependencies.
  id("flink-job-conventions")
}

repositories { mavenCentral() }

dependencies {
  implementation(project(":shared"))
  implementation("org.knowm.xchart:xchart:3.8.7")
  implementation("org.apache.commons:commons-math:2.2")
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
            ?: "at.ac.uibk.dps.streamprocessingapplications.etl.FlinkJobTAXI"
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
