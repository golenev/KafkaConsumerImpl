package configs

import com.validator.app.model.MissingHeadersPayload
import com.validator.app.model.ValidatedPayload
import com.validator.e2e.kafka.consumer.ConsumerKafkaConfig

private const val OUTPUT_TOPIC = "out_validator"

val validatorOutputConsumerConfig: ConsumerKafkaConfig =
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
        awaitClazz = ValidatedPayload::class.java
        awaitLastNPerPartition = 0
    }

val validatorOutputMissingHeadersConsumerConfig: ConsumerKafkaConfig =
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
        awaitLastNPerPartition = 0
    }
