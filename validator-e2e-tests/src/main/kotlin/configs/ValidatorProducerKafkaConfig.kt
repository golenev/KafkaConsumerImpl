package configs

import com.validator.e2e.kafka.producer.ProducerKafkaConfig
import java.util.UUID

const val VALIDATOR_INPUT_TOPIC = "in_validator"

val validatorInputProducerConfig: ProducerKafkaConfig =
    ProducerKafkaConfig(
        bootstrapServers = "localhost:9092",
        username = "validator-user",
        password = "validator-password",
    ).apply {
        securityProtocol = "PLAINTEXT"
        clientId = "validator-e2e-tests-${UUID.randomUUID()}"
    }
