# Developing an Application
This project is structured in the following way:
Each application is its own Gradle [subproject](https://docs.gradle.org/current/userguide/multi_project_builds.html).
For Flink applications there exists a [Gradle conventions plugin](https://docs.gradle.org/current/samples/sample_convention_plugins.html) called `flink-job-conventions`. This plugin manages basic dependencies like the Beam core library and the Flink runner as well as testing dependencies.

The root `build.gradle.kts` file contains the config for the [Spotless](https://github.com/diffplug/spotless) formatter.

Each subproject has its own `build.gradle.kts` file and needs to be added to the `settings.gradle.kts` file.
For a Flink application subproject a minimal `build.gradle.kts` file might look like this:

```kotlin
plugins {
    id("java")
    // INFO: This plugin is in `buildSrc` and manages shared dependencies.
    id("flink-job-conventions")
}

val mainClassName = // TODO: Add the java main class name string.

application {
    mainClass = mainClassName
}

repositories {
    mavenCentral()
}

dependencies {
    // TODO: Add application specific dependencies.
}

tasks.named<Jar>("jar") {
    archiveBaseName.set(/* TODO: Specify the archive name. */)
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
```

After that Gradle will automatically detect the new subproject and build it when calling `./gradlew build` from the project root.
`Spotless` formatting will also be enforced.
