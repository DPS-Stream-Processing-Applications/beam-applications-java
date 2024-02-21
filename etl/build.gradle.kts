plugins {
    id("application")
    // INFO: This plugin is in `buildSrc` and manages shared dependencies.
    id("flink-job-conventions")
}

val mainClassName = "at.ac.uibk.dps.streamprocessingapplications.EtlJob"

application {
    mainClass = mainClassName
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.named<Jar>("jar") {
    archiveBaseName.set("EtlJob")
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
