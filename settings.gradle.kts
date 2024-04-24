plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version("0.7.0")
}
rootProject.name = "beam-applications-java"

include("etl")
include("shared")
include("stats")
include("pred")
include("kafkaProducer")
include("train")
