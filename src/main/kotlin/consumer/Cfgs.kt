package consumer

import com.fasterxml.jackson.databind.ObjectMapper
import consumer.service.ConsumerKafkaConfig

val someConsumerConfig: ConsumerKafkaConfig =
    ConsumerKafkaConfig(
        bootstrapServers = "",
        username = "replace-me",
        password = "replace-me",
    ).apply {
        securityProtocol = "SASL_PLAINTEXT"
        saslMechanism = "SCRAM-SHA-256"
        groupId = null
        groupIdPrefix = null
        autoCommit = false
        awaitTopic = "some_topic"
        awaitMapper = ObjectMapper()
        awaitClazz = Any::class.java
    }



