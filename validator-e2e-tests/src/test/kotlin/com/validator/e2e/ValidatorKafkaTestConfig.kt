package com.validator.e2e

import consumer.service.ConsumerKafkaConfig
import producer.service.ProducerKafkaConfig
import java.util.UUID

data class ValidatorKafkaSettings(
    val bootstrapServers: String,
    val inputTopic: String,
    val outputTopic: String,
    val username: String,
    val password: String,
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

val validatorKafkaSettings = ValidatorKafkaSettings(
    bootstrapServers = "localhost:9092",
    inputTopic = "in_validator",
    outputTopic = "out_validator",
    username = "validator-user",
    password = "validator-password",
    securityProtocol = "SASL_PLAINTEXT",
    saslMechanism = "SCRAM-SHA-256",
)
