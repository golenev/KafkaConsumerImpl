package com.validator.e2e

import consumer.service.ConsumerKafkaConfig
import producer.service.ProducerKafkaConfig
import java.util.UUID

data class ValidatorKafkaSettings(
    val bootstrapServers: String,
    val inputTopic: String,
    val outputTopic: String,
    val username: String?,
    val password: String?,
    val securityProtocol: String,
    val saslMechanism: String?,
) {
    fun createProducerConfig(): ProducerKafkaConfig =
        ProducerKafkaConfig(
            bootstrapServers = bootstrapServers,
            username = username,
            password = password,
        ).apply {
            securityProtocol = this@ValidatorKafkaSettings.securityProtocol
            this@ValidatorKafkaSettings.saslMechanism?.let { saslMechanism = it }
            clientId = "validator-e2e-tests-${UUID.randomUUID()}"
        }

    fun createConsumerConfig(): ConsumerKafkaConfig =
        ConsumerKafkaConfig(
            bootstrapServers = bootstrapServers,
            username = username,
            password = password,
        ).apply {
            securityProtocol = this@ValidatorKafkaSettings.securityProtocol
            this@ValidatorKafkaSettings.saslMechanism?.let { saslMechanism = it }
            groupIdPrefix = "validator-e2e-"
            autoCommit = false
        }
}

private fun envOrNull(key: String): String? =
    System.getenv(key)?.takeUnless { it.isBlank() }

private val defaultSecurityProtocol =
    envOrNull("VALIDATOR_TEST_KAFKA_SECURITY_PROTOCOL") ?: "PLAINTEXT"

private val defaultSaslMechanism = envOrNull("VALIDATOR_TEST_KAFKA_SASL_MECHANISM")
    ?: if (defaultSecurityProtocol.uppercase().startsWith("SASL")) "SCRAM-SHA-256" else null

val validatorKafkaSettings = ValidatorKafkaSettings(
    bootstrapServers = envOrNull("VALIDATOR_TEST_KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092",
    inputTopic = envOrNull("VALIDATOR_TEST_KAFKA_TOPIC_INPUT") ?: "in_validator",
    outputTopic = envOrNull("VALIDATOR_TEST_KAFKA_TOPIC_OUTPUT") ?: "out_validator",
    username = envOrNull("VALIDATOR_TEST_KAFKA_USERNAME") ?: "validator-user",
    password = envOrNull("VALIDATOR_TEST_KAFKA_PASSWORD") ?: "validator-password",
    securityProtocol = defaultSecurityProtocol,
    saslMechanism = defaultSaslMechanism,
)
