package configs // Помещает файл в пакет configs.

import com.validator.e2e.kafka.consumer.ConsumerKafkaConfig

private const val BATCHED_OUTPUT_TOPIC = "batched_output" // Объявляет имя batched-выходного топика для сценариев пакетной обработки.

fun validatorBatchedOutputConsumerConfig(deserializerClass: Class<*>): ConsumerKafkaConfig = // Объявляет функцию построения конфига батчевого консюмера с заданным десериализатором.
    ConsumerKafkaConfig(
        bootstrapServers = "localhost:9092",
        username = "validator-user",
        password = "validator-password",
    ).apply {
        securityProtocol = "PLAINTEXT"
        groupId = null // Сбрасывает фиксированный groupId, чтобы не использовать конкретную consumer group.
        groupIdPrefix = null // Сбрасывает префикс groupId, отключая автогенерацию группы с префиксом.
        autoCommit = false // Отключает авто-коммит offset'ов для ручного/контролируемого чтения.
        awaitTopic = BATCHED_OUTPUT_TOPIC
        awaitMapper = ObjectMapper.globalMapper
        this.deserializerClass = deserializerClass
        // В batched-сценариях сервис публикует сообщения быстрее (без искусственных задержек), // Поясняет причину увеличенного окна чтения сообщений.
        // поэтому читаем последние N сообщений, чтобы избежать race при старте консюмера. // Поясняет, что это снижает риск race condition при старте.
        awaitLastNPerPartition = 200 // Читает последние 200 сообщений на партицию при ожидании.
    }
