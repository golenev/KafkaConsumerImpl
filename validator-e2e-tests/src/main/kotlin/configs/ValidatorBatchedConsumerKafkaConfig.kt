package configs // Помещает файл в пакет configs.

import com.validator.e2e.kafka.consumer.ConsumerKafkaConfig

private const val OUTPUT_TOPIC = "out_validator" // Объявляет имя обычного выходного топика (константа в пределах файла).
private const val BATCHED_OUTPUT_TOPIC = "batched_output" // Объявляет имя batched-выходного топика для сценариев пакетной обработки.

fun validatorBatchedOutputConsumerConfig(deserializerClass: Class<*>): ConsumerKafkaConfig = // Объявляет функцию построения конфига батчевого консюмера с заданным десериализатором.
    ConsumerKafkaConfig( // Создаёт базовый объект ConsumerKafkaConfig.
        bootstrapServers = "localhost:9092", // Устанавливает адрес Kafka-брокера.
        username = "validator-user", // Устанавливает username для подключения к Kafka.
        password = "validator-password", // Устанавливает password для подключения к Kafka.
    ).apply { // Переходит к настройке созданного конфига через apply.
        securityProtocol = "PLAINTEXT" // Выставляет протокол безопасности как PLAINTEXT.
        groupId = null // Сбрасывает фиксированный groupId, чтобы не использовать конкретную consumer group.
        groupIdPrefix = null // Сбрасывает префикс groupId, отключая автогенерацию группы с префиксом.
        autoCommit = false // Отключает авто-коммит offset'ов для ручного/контролируемого чтения.
        awaitTopic = BATCHED_OUTPUT_TOPIC // Указывает топик ожидания сообщений для batched-сценария.
        awaitMapper = ObjectMapper.globalMapper // Назначает глобальный ObjectMapper для десериализации ожидаемых сообщений.
        this.deserializerClass = deserializerClass // Передаёт класс десериализатора в конфиг консюмера.
        // В batched-сценариях сервис публикует сообщения быстрее (без искусственных задержек), // Поясняет причину увеличенного окна чтения сообщений.
        // поэтому читаем последние N сообщений, чтобы избежать race при старте консюмера. // Поясняет, что это снижает риск race condition при старте.
        awaitLastNPerPartition = 200 // Читает последние 200 сообщений на партицию при ожидании.
    } // Завершает блок дополнительной настройки ConsumerKafkaConfig.
