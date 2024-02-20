plugins {
    id("application")
    id("com.diffplug.spotless") version "6.25.0"
}

repositories {
    mavenCentral()
}

dependencies {
    // App dependencies.
    implementation("org.apache.beam:beam-sdks-java-core:2.54.0")
    implementation("org.apache.beam:beam-runners-direct-java:2.54.0")
    implementation("org.apache.beam:beam-runners-flink-1.16:2.54.0")
    implementation("org.slf4j:slf4j-jdk14:1.7.32")

    // Tests dependencies.
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.hamcrest:hamcrest:2.2")
}

application {
    mainClass = "com.example.App"
}

spotless {

    format("misc", {
        // define the files to apply `misc` to
        target("*.gradle", ".gitattributes", ".gitignore")

        // define the steps to apply to those files
        trimTrailingWhitespace()
        indentWithTabs() // or spaces. Takes an integer argument if you don't like 4
        endWithNewline()
    })

    kotlinGradle {
        ktlint()
    }

    java {
        removeUnusedImports()
        importOrder()
        googleJavaFormat("1.8").aosp().reflowLongStrings()
        formatAnnotations()
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
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
