package configs

import com.validator.e2e.kafka.consumer.ConsumerKafkaConfig

private const val OUTPUT_TOPIC = "out_validator"
private const val BATCHED_OUTPUT_TOPIC = "batched_output"

fun validatorBatchedOutputConsumerConfig(deserializerClass: Class<*>): ConsumerKafkaConfig =
    ConsumerKafkaConfig(
        bootstrapServers = "localhost:9092",
        username = "validator-user",
        password = "validator-password",
    ).apply {
        securityProtocol = "PLAINTEXT"
        groupId = null
        groupIdPrefix = null
        autoCommit = false
        awaitTopic = BATCHED_OUTPUT_TOPIC
        awaitMapper = ObjectMapper.globalMapper
        this.deserializerClass = deserializerClass
        // В batched-сценариях сервис публикует сообщения быстрее (без искусственных задержек),
        // поэтому читаем последние N сообщений, чтобы избежать race при старте консюмера.
        awaitLastNPerPartition = 200
    }

