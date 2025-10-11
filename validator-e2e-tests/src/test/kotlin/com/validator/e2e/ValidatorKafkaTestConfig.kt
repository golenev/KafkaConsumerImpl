package com.validator.e2e

import consumer.service.ConsumerKafkaConfig
import producer.service.ProducerKafkaConfig
import java.util.UUID

private fun envOrNull(name: String): String? =
    System.getenv(name)?.takeIf { it.isNotBlank() }

private fun envOrThrow(name: String): String =
    envOrNull(name)
        ?: error("Environment variable $name must be provided for validator tests")

data class ValidatorKafkaSettings(
    val bootstrapServers: String,
    val inputTopic: String,
    val outputTopic: String,
    val username: String?,
    val password: String?,
    val securityProtocol: String,
    val saslMechanism: String,
) {
    fun createProducerConfig(): ProducerKafkaConfig =
        ProducerKafkaConfig(
            bootstrapServers = bootstrapServers,
            username = username,
            password = password,
        ).apply {
            securityProtocol = this@ValidatorKafkaSettings.securityProtocol
            saslMechanism = this@ValidatorKafkaSettings.saslMechanism
            clientId = "validator-e2e-tests-${UUID.randomUUID()}"
        }

    fun createConsumerConfig(): ConsumerKafkaConfig =
        ConsumerKafkaConfig(
            bootstrapServers = bootstrapServers,
            username = username,
            password = password,
        ).apply {
            securityProtocol = this@ValidatorKafkaSettings.securityProtocol
            saslMechanism = this@ValidatorKafkaSettings.saslMechanism
            groupIdPrefix = "validator-e2e-"
            autoCommit = false
        }
}

fun loadValidatorKafkaSettings(): ValidatorKafkaSettings {
    val bootstrapServers = envOrThrow("VALIDATOR_KAFKA_BOOTSTRAP")
    val inputTopic = envOrThrow("VALIDATOR_TOPIC_INPUT")
    val outputTopic = envOrThrow("VALIDATOR_TOPIC_OUTPUT")
    val securityProtocol = envOrNull("VALIDATOR_KAFKA_SECURITY_PROTOCOL")
    val saslMechanism = envOrNull("VALIDATOR_KAFKA_SASL_MECHANISM")
    val username = envOrNull("VALIDATOR_KAFKA_USERNAME")
    val password = envOrNull("VALIDATOR_KAFKA_PASSWORD")
    val resolvedSecurityProtocol = securityProtocol
        ?: if (!username.isNullOrBlank() && !password.isNullOrBlank()) "SASL_PLAINTEXT" else "PLAINTEXT"
    val resolvedSaslMechanism = saslMechanism ?: "SCRAM-SHA-256"

    return ValidatorKafkaSettings(
        bootstrapServers = bootstrapServers,
        inputTopic = inputTopic,
        outputTopic = outputTopic,
        username = username,
        password = password,
        securityProtocol = resolvedSecurityProtocol,
        saslMechanism = resolvedSaslMechanism,
    )
}

val validatorKafkaSettings: ValidatorKafkaSettings = loadValidatorKafkaSettings()
