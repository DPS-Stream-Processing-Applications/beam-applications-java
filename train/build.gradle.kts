plugins {
    id("application")
    // INFO: This plugin is in `buildSrc` and manages shared dependencies.
    id("flink-job-conventions")
}

val mainClassName = "at.ac.uibk.dps.streamprocessingapplications.TrainJob"

application {
    mainClass = mainClassName
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    // https://mvnrepository.com/artifact/org.apache.beam/beam-sdks-java-core
    implementation("org.apache.beam:beam-sdks-java-core:2.45.0")
    implementation("com.microsoft.azure:azure-storage:8.6.6")
    // https://mvnrepository.com/artifact/org.apache.beam/beam-runners-direct-java
    runtimeOnly("org.apache.beam:beam-runners-direct-java:2.45.0")
    implementation("nz.ac.waikato.cms.weka:weka-stable:3.6.6")
    implementation("com.opencsv:opencsv:3.3")
    implementation("org.apache.commons:commons-math3:3.5")
    implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.0.2")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.named<Jar>("jar") {
    archiveBaseName.set("TrainJob")
    destinationDirectory.set(file("build"))
    manifest {
        attributes(
            "Main-Class" to mainClassName,
        )
    }
    from("src/main/resources") {
        into("resources")
    }
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    isZip64 = true
}
