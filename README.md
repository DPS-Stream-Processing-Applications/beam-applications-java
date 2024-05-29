<div align="center">
<!-- INFO: The empty line is required for center to work.-->

[![Nix](https://img.shields.io/badge/Nix_devShell-%235277C3?style=for-the-badge&logo=NixOS&logoColor=white)](https://nixos.wiki/wiki/Flakes)
[![Gradle](https://img.shields.io/badge/Gradle%208.6-02303A?style=for-the-badge&logo=Gradle&logoColor=white)](https://docs.gradle.org/8.6/userguide/userguide.html)
[![Open JDK](https://img.shields.io/badge/OpenJDK%2011.0.19-%23437291?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/projects/jdk/11/)
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
To install Nix follow the [official instructions](https://nixos.org/download) for your platform.
Following this, you need to enable `flakes` and `nix-command` for the Nix package manager that you just installed.
The "Other Distros, without Home-Manager" section of the [Flake Wiki](https://nixos.wiki/wiki/Flakes) will explain how to do this.
> [!TIP]
> If the `~/.config/nix` folder and `nix.conf` file do not already exist after installing, you need to create them manually.

## Nix Develop
After successfully installing Nix and enabling Flakes, you will be able to use the `nix develop` command in the root of the project to enter a
development shell managed by Nix. To exit the dev shell, use the `exit` command or hit `Ctrl+d`.

## Direnv
> [!NOTE]
> This step is entirely optional but may improve your development experience.

Using [Direnv](https://direnv.net/) will allow you to automatically launch the Nix `devShell` whenever you change into the project directory.
Direnv can also be detected by your IDE if a plugin exists.
You might also want to install [nix-direnv](https://github.com/nix-community/nix-direnv) to improve the Direnv experience with Nix.

## Gradle
This project makes use of the Gradle wrapper to provide every developer with the same version of Gradle. No global Gradle installation is needed.
Use the `gradlew` binary for every Gradle command.

# Dev Environment

## Local Flink Cluster
If you want to deploy a standalone Flink job with no dependencies on external sources or sinks,
you can do this via the available scripts `start-cluster.sh` and `stop-cluster.sh`.
These scripts are available from your `PATH` once you are in the nix development shell. 

>[!IMPORTANT]
>Running the `start-cluster.sh` command for the first time might lead to the following error:
>```bash
> /nix/store/hai1b6r3l172yp49pbyj5z4zpmn8i9k1-flink-1.18.1/opt/flink/bin/flink-daemon.sh: line 139: /tmp/flink-logs/flink-<user-name>-standalonesession-0-<host-name>.out: No such file or directory
>```
> Invoking `flink --version` once appears to solve this issue.

## Kubernetes
For all the applications in this repository external resources are required.
These resources are all managed within a Kubernetes cluster.

### Local K8S Cluster
> [!IMPORTANT]
> Make sure you have [docker](https://www.docker.com/) installed on your system before reading further.

The nix dev shell provides `k3d` as means to spin up a local Kubernetes cluster.
Follow the [quick start guide](https://k3d.io/v5.6.3/#quick-start) to set up an empty cluster.

### Helm Deployment
All the applications of this repository depend on external resources like an Apache `Kafka` cluster as well as a `mongoDB` database.
All dependencies for the applications are managed and deployed via a custom `helm chart` in the `helm-charts` directory.
Refer to the [README](./helm-charts/riot-applications/README.md) for an installation walkthrough.

## Running a Job with Flink
The nix development environment provides a Flink binary installation to be used for deploying Flink jobs.
Each subproject has a `jar` build task that builds the `.jar` file of the job that can then be submitted to a Flink cluster.
The following commands will enter the development envrionment, build all applications, start a local Flink cluster
and submit the `ETLJob.jar` job to the Flink cluster.

>[!WARNING]
> Make sure you have either the [Local Flink Cluster](#local-flink-cluster) or the [Kubernetes deployment](#kubernetes)
> set up before attempting to run a Flink job.

```bash
nix develop # INFO: Not needed if already in a nix shell or using direnv.
./gradlew build
flink run ./etl/build/FlinkJob.jar
```

