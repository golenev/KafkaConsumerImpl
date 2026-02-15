package configs // Помещает файл в пакет configs.

import com.validator.e2e.kafka.producer.ProducerKafkaConfig
import java.util.UUID

const val VALIDATOR_INPUT_TOPIC = "in_validator" // Объявляет имя входного топика валидатора как публичную константу.

fun validatorInputProducerConfig(): ProducerKafkaConfig = // Объявляет функцию построения конфига продюсера для отправки входных сообщений.
    ProducerKafkaConfig( // Создаёт базовый объект ProducerKafkaConfig.
        bootstrapServers = "localhost:9092", // Устанавливает адрес Kafka-брокера.
        username = "validator-user", // Устанавливает username для подключения к Kafka.
        password = "validator-password", // Устанавливает password для подключения к Kafka.
    ).apply { // Переходит к дополнительной настройке созданного конфига через apply.
        securityProtocol = "PLAINTEXT" // Выставляет протокол безопасности как PLAINTEXT.
        clientId = "validator-e2e-tests-${UUID.randomUUID()}" // Формирует уникальный clientId для изоляции producer-сессий в тестах.
    } // Завершает блок настройки ProducerKafkaConfig.
