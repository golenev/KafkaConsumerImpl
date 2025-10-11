package com.validator.app.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.bind.DefaultValue

@ConfigurationProperties("validator.topics")
data class ValidatorTopicsProperties(
    @DefaultValue("in_validator")
    val input: String,
    @DefaultValue("out_validator")
    val output: String,
    @DefaultValue("3")
    val partitions: Int,
    @DefaultValue("1")
    val replicationFactor: Short
)

@ConfigurationProperties("validator.kafka")
data class ValidatorKafkaProperties(
    @DefaultValue("localhost:8817")
    val bootstrapServers: String,
    val username: String? = null,
    val password: String? = null,
    val securityProtocol: String? = null,
    val saslMechanism: String? = null,
    val groupId: String? = null,
    val groupIdPrefix: String? = "validator-",
    val groupInstanceId: String? = null,
    @DefaultValue("earliest")
    val autoOffsetReset: String,
    @DefaultValue("3")
    val concurrency: Int
)
