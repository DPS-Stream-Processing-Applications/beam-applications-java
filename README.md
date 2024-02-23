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
with nix. The nix shell will provide an installation of Flink as well as the OpenJDK needed for the Gradle wrapper to work.

## Installing Nix
To install nix follow the [official instructions](https://nixos.org/download).
Following this, you need to enable `flakes` and `nix-command` for the nix package manager that you just installed.
The "Other Distros, without Home-Manager" section of the [Flake Wiki](https://nixos.wiki/wiki/Flakes) will explain how to do this.

## Nix Develop
After successfully installing nix and enabling flakes, you will be able to use the `nix develop` command in the root of the project to enter a
development shell managed by nix. To exit the dev shell run `exit` or hit `Ctrl+d`.

## Direnv
Using [direnv](https://direnv.net/) will allow you to automatically launch the nix `devShell` whenever you change into the project directory.
Direnv can also be detected by your IDE if a plugin exists. 

## Gradle
This project makes use of the Gradle wrapper to provide every developer with the same version of Gradle. No global Gradle installation is needed.
Use the `gradlew` binary for every Gradle command.

# Running a Job with Flink
The nix development environment provides a Flink binary installation to be used for deploying flink jobs.
Each subproject has a `jar` build task that builds the `.jar` file of the job that can then be submitted to a Flink cluster.

For example:
```bash
./gradlew build
flink run -m localhost:8081 ./etl/build/EtlJob.jar
```
Will submit the `etl` job to a Flink cluster running locally.

# Developing an Application
 TODO: add development workflow
