plugins {
    kotlin("jvm")
}

group = "com.validator"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    val jacksonVersion = "2.17.2"

    testImplementation(kotlin("test"))
    testImplementation(project(":"))
    testImplementation(project(":validator-service"))
    testImplementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    testImplementation("io.kotest:kotest-assertions-core:5.9.1")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(21)
}
