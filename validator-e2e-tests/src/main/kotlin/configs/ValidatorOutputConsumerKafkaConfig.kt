package configs

import com.validator.e2e.kafka.consumer.ConsumerKafkaConfig

private const val OUTPUT_TOPIC = "out_validator"

fun validatorOutputConsumerConfig(deserializerClass: Class<*>): ConsumerKafkaConfig =
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
        this.deserializerClass = deserializerClass
        awaitLastNPerPartition = 0
    }
