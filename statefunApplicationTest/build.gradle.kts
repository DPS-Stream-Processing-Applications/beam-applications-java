plugins {
  id("java")
  // INFO: This plugin is in `buildSrc` and manages shared dependencies.
  id("flink-job-conventions")
}

val mainClassName = "org.example.UndertowMain"

repositories { mavenCentral() }

dependencies {
  // https://mvnrepository.com/artifact/org.apache.flink/statefun-sdk-java
  implementation("org.apache.flink:statefun-sdk-java:3.3.0")
  // https://mvnrepository.com/artifact/io.undertow/undertow-core
  implementation("io.undertow:undertow-core:2.3.13.Final")
}

tasks.named<Jar>("jar") {
  archiveBaseName.set("statefunJob")
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
