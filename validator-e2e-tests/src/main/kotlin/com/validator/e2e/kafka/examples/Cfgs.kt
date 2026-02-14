package com.validator.e2e.kafka.examples

import com.fasterxml.jackson.databind.ObjectMapper
import com.validator.e2e.kafka.consumer.ConsumerKafkaConfig

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



