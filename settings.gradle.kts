pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name = "KafkaConsumerImpl"
include("validator-service")
include("validator-e2e-tests")

