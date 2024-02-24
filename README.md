<div align="center">
<!-- NOTE: The empty line is required for center to work.-->

[![Nix](https://img.shields.io/badge/Nix_devShell-%235277C3?style=for-the-badge&logo=NixOS&logoColor=white)](https://nixos.wiki/wiki/Flakes)
[![Gradle](https://img.shields.io/badge/Gradle%208.6-02303A?style=for-the-badge&logo=Gradle&logoColor=white)](https://docs.gradle.org/8.6/userguide/userguide.html)
[![Open JDK](https://img.shields.io/badge/OpenJDK%2011.0.19-%23437291?style=for-the-badge&logo=openjdk&logoColor=white)](https://flink.apache.org/)
[![Apache Beam](https://custom-icon-badges.demolab.com/badge/Apache%20Beam%202.54-orange?style=for-the-badge&logo=apache-beam&logoColor=white)](https://beam.apache.org/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink%201.18.1-E6526F?style=for-the-badge&logo=Apache%20Flink&logoColor=white)](https://flink.apache.org/)
</div>

# Description
This project is a collection of applications written with `Apache Beam`.
The applications are targeted to be run with `Apache Flink` but should be reusable for any of the other supported runners.

# Setup
This project contains a `flake.nix` file to manage all the dependencies that would usually need to be installed through a regular package manager
with the Nix package manager instead. The Nix Shell will provide an installation of Flink as well as the OpenJDK needed for the Gradle wrapper to work.

## Installing Nix
To install Nix follow the [official instructions](https://nixos.org/download).
Following this, you need to enable `flakes` and `nix-command` for the Nix package manager that you just installed.
The "Other Distros, without Home-Manager" section of the [Flake Wiki](https://nixos.wiki/wiki/Flakes) will explain how to do this.
> [!NOTE]
> If the `~/.config/nix` folder and `nix.conf` file do not already exist after installing, you need to create them manually.

## Nix Develop
After successfully installing Nix and enabling Flakes, you will be able to use the `nix develop` command in the root of the project to enter a
development shell managed by Nix. To exit the dev shell, use the `exit` command or hit `Ctrl+d`.

## Direnv
Using [Direnv](https://direnv.net/) will allow you to automatically launch the nix `devShell` whenever you change into the project directory.
Direnv can also be detected by your IDE if a plugin exists.
You might also want to install [nix-direnv](https://github.com/nix-community/nix-direnv) to improve the Direnv experience with Nix.

## Gradle
This project makes use of the Gradle wrapper to provide every developer with the same version of Gradle. No global Gradle installation is needed.
Use the `gradlew` binary for every Gradle command.

# Running a Job with Flink
The nix development environment provides a Flink binary installation to be used for deploying flink jobs.
Each subproject has a `jar` build task that builds the `.jar` file of the job that can then be submitted to a Flink cluster.

For example:
```bash
nix develop # INFO: Not needed if already in a nix shell or using direnv.
./gradlew build
start-cluster.sh
flink run -m localhost:8081 ./etl/build/EtlJob.jar
stop-cluster.sh
```
Will build all applications, start a local Flink cluster and submit the `ETLJob.jar` job to the locally running cluster.

# Developing an Application
This project is structured in the following way:
Each application is its own Gradle [subproject](https://docs.gradle.org/current/userguide/multi_project_builds.html).
For Flink applications there exists a [Gradle conventions plugin](https://docs.gradle.org/current/samples/sample_convention_plugins.html) called `flink-job-conventions`. This plugin manages basic dependencies like the Beam core library and the Flink runner as well as testing dependencies.

The root `build.gradle.kts` file contains the config for the [Spotless](https://github.com/diffplug/spotless) formatter.

Each subproject has its own `build.gradle.kts` file and needs to be added to the `settings.gradle.kts` file.
For a Flink application subproject a minimal `build.gradle.kts` file might look like this:

```kotlin
plugins {
    id("application")
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

tasks.named<Test>("test") {
    useJUnitPlatform()
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
