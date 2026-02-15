package configs // Помещает файл в пакет configs.

import com.validator.e2e.kafka.consumer.ConsumerKafkaConfig

private const val OUTPUT_TOPIC = "out_validator"

fun validatorOutputConsumerConfig(deserializerClass: Class<*>): ConsumerKafkaConfig =
    ConsumerKafkaConfig(
        bootstrapServers = "localhost:9092",
        username = "validator-user",
        password = "validator-password",
    ).apply {
        securityProtocol = "PLAINTEXT"
        groupId = null // Сбрасывает фиксированный groupId, чтобы не привязываться к конкретной consumer group.
        groupIdPrefix = null // Сбрасывает префикс groupId, отключая автогенерацию группы с префиксом.
        autoCommit = false // Отключает авто-коммит offset'ов для контролируемого чтения.
        awaitTopic = OUTPUT_TOPIC
        awaitMapper = ObjectMapper.globalMapper
        this.deserializerClass = deserializerClass // Передаёт в конфиг класс десериализатора входящего сообщения.
        awaitLastNPerPartition = 0 // Отключает чтение последних N сообщений, ожидая только новые записи.
    }
