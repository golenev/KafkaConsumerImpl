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
    val springBootVersion = "3.3.5"
    val kafkaVersion = "3.7.0"
    val slf4jVersion = "2.0.16"

    implementation(project(":validator-service"))
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    implementation("org.slf4j:slf4j-api:$slf4jVersion")
    runtimeOnly("org.slf4j:slf4j-simple:$slf4jVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    testImplementation(kotlin("test"))
    testImplementation(platform("org.springframework.boot:spring-boot-dependencies:$springBootVersion"))
    testImplementation("io.kotest:kotest-assertions-core:5.9.1")
    testImplementation("io.qameta.allure:allure-junit5:2.29.1")
}

tasks.test {
    useJUnitPlatform()
    systemProperty("allure.results.directory", layout.buildDirectory.dir("allure-results").get().asFile.absolutePath)
}

kotlin {
    jvmToolchain(21)
}
