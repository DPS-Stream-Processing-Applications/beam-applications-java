plugins {
  id("java")
  // INFO: This plugin is in `buildSrc` and manages shared dependencies.
  id("flink-job-conventions")
}

repositories { mavenCentral() }

dependencies { implementation("org.json:json:20240205") }
