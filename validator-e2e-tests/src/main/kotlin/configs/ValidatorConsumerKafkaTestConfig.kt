package configs

import consumer.service.ConsumerKafkaConfig

class ValidatorConsumerKafkaSettings(
    val bootstrapServers: String = "localhost:9092",
    val outputTopic: String = "out_validator",
    val username: String? = "validator-user",
    val password: String? = "validator-password",
    val securityProtocol: String = "PLAINTEXT",
    val saslMechanism: String? = null,
) {
    fun createConsumerConfig(): ConsumerKafkaConfig =
        ConsumerKafkaConfig(
            bootstrapServers = bootstrapServers,
            username = username,
            password = password,
        ).apply {
            securityProtocol = this@ValidatorConsumerKafkaSettings.securityProtocol
            this@ValidatorConsumerKafkaSettings.saslMechanism?.let { saslMechanism = it }
            groupIdPrefix = "validator-e2e-"
            autoCommit = false
        }
}
