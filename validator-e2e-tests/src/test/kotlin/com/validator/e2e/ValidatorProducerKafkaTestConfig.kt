package com.validator.e2e

import producer.service.ProducerKafkaConfig
import java.util.UUID

class ValidatorProducerKafkaSettings(
    val bootstrapServers: String = "localhost:9092",
    val inputTopic: String = "in_validator",
    val username: String? = "validator-user",
    val password: String? = "validator-password",
    val securityProtocol: String = "PLAINTEXT",
    val saslMechanism: String? = null,
) {
    fun createProducerConfig(): ProducerKafkaConfig =
        ProducerKafkaConfig(
            bootstrapServers = bootstrapServers,
            username = username,
            password = password,
        ).apply {
            securityProtocol = this@ValidatorProducerKafkaSettings.securityProtocol
            this@ValidatorProducerKafkaSettings.saslMechanism?.let { saslMechanism = it }
            clientId = "validator-e2e-tests-${UUID.randomUUID()}"
        }
}
