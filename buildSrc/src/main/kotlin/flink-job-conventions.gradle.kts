plugins {
    id("java")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.beam:beam-sdks-java-core:2.54.0")
    implementation("org.apache.beam:beam-runners-direct-java:2.54.0")
    implementation("org.apache.beam:beam-runners-flink-1.16:2.54.0")
    implementation("org.slf4j:slf4j-jdk14:1.7.32")

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.hamcrest:hamcrest:2.2")
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}
