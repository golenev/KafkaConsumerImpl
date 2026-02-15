package configs // Помещает файл в пакет configs.

import com.validator.e2e.kafka.producer.ProducerKafkaConfig
import java.util.*

const val VALIDATOR_INPUT_TOPIC = "in_validator"

fun validatorInputProducerConfig(): ProducerKafkaConfig =
    ProducerKafkaConfig(
        bootstrapServers = "localhost:9092",
        username = "validator-user",
        password = "validator-password",
    ).apply {
        securityProtocol = "PLAINTEXT"
        clientId = "validator-e2e-tests-${UUID.randomUUID()}" // Формирует уникальный clientId для изоляции producer-сессий в тестах.
    }
