package configs // Помещает файл в пакет configs.

import com.validator.e2e.kafka.consumer.ConsumerKafkaConfig

private const val OUTPUT_TOPIC = "out_validator" // Объявляет имя выходного топика для стандартного сценария в пределах файла.

fun validatorOutputConsumerConfig(deserializerClass: Class<*>): ConsumerKafkaConfig = // Объявляет функцию построения конфига обычного output-консюмера.
    ConsumerKafkaConfig( // Создаёт базовый объект ConsumerKafkaConfig.
        bootstrapServers = "localhost:9092", // Устанавливает адрес Kafka-брокера.
        username = "validator-user", // Устанавливает username для подключения к Kafka.
        password = "validator-password", // Устанавливает password для подключения к Kafka.
    ).apply { // Переходит к настройке созданного конфига через apply.
        securityProtocol = "PLAINTEXT" // Выставляет протокол безопасности как PLAINTEXT.
        groupId = null // Сбрасывает фиксированный groupId, чтобы не привязываться к конкретной consumer group.
        groupIdPrefix = null // Сбрасывает префикс groupId, отключая автогенерацию группы с префиксом.
        autoCommit = false // Отключает авто-коммит offset'ов для контролируемого чтения.
        awaitTopic = OUTPUT_TOPIC // Указывает выходной топик, из которого нужно ожидать сообщения.
        awaitMapper = ObjectMapper.globalMapper // Назначает глобальный ObjectMapper для десериализации ожидаемых сообщений.
        this.deserializerClass = deserializerClass // Передаёт в конфиг класс десериализатора входящего сообщения.
        awaitLastNPerPartition = 0 // Отключает чтение последних N сообщений, ожидая только новые записи.
    } // Завершает блок дополнительной настройки ConsumerKafkaConfig.
