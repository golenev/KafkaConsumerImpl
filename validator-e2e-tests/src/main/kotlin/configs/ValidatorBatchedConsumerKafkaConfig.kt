package configs

import com.validator.app.model.MissingHeadersPayload
import com.validator.app.model.ValidatedPayload
import com.validator.e2e.kafka.consumer.ConsumerKafkaConfig

private const val OUTPUT_TOPIC = "out_validator"
private const val BATCHED_OUTPUT_TOPIC = "batched_output"

val validatorBatchedOutputConsumerConfig: ConsumerKafkaConfig =
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
        awaitClazz = ValidatedPayload::class.java
        // В batched-сценариях сервис публикует сообщения быстрее (без искусственных задержек),
        // поэтому читаем последние N сообщений, чтобы избежать race при старте консюмера.
        awaitLastNPerPartition = 200
    }

val validatorBatchedMissingHeadersConsumerConfig: ConsumerKafkaConfig =
    ConsumerKafkaConfig(
        bootstrapServers = "localhost:9092",
        username = "validator-user",
        password = "validator-password",
    ).apply {
        securityProtocol = "PLAINTEXT"
        groupId = null
        groupIdPrefix = null
        autoCommit = false
        awaitTopic = OUTPUT_TOPIC
        awaitMapper = ObjectMapper.globalMapper
        awaitClazz = MissingHeadersPayload::class.java
        awaitLastNPerPartition = 200
    }
