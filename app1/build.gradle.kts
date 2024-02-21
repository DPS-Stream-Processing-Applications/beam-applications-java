plugins {
    id("application")
    // INFO: This plugin is in `buildSrc` and manages shared dependencies.
    id("flink-job-conventions")
}

application {
    mainClass = "com.example.App"
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.named<Jar>("jar") {
    archiveBaseName.set("app1")
    destinationDirectory.set(file("build"))
    manifest {
        attributes(
            "Main-Class" to "com.example.App",
        )
    }
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    isZip64 = true
}
