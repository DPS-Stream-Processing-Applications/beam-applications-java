plugins {
    id("application")
    // INFO: This plugin is in `buildSrc` and manages shared dependencies.
    id("flink-job-conventions")
}

val mainClassName = "at.ac.uibk.dps.streamprocessingapplications.PredJob"

application {
    mainClass = mainClassName
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.json:json:20240205")
    implementation("com.microsoft.azure:azure-storage:4.0.0")
    implementation("nz.ac.waikato.cms.weka:weka-stable:3.6.6")
    implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.0.2")
    implementation("com.opencsv:opencsv:3.3")
    implementation("com.googlecode.json-simple:json-simple:1.1")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.named<Jar>("jar") {
    archiveBaseName.set("PredJob")
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
