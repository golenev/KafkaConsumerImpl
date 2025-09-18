package com.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.*

open class KafkaConfig(
    protected val bootstrapServers: String,
    protected val username: String,
    protected val password: String
) {
    var saslMechanism: String = "SCRAM-SHA-256"
        set(value) {
            field = value
            jaasModule = when (value) {
                "PLAIN" -> JAAS_PLAIN_MODULE
                "SCRAM-SHA-256", "SCRAM-SHA-512" -> JAAS_SCRAM_MODULE
                else -> JAAS_SCRAM_MODULE
            }
        }

    var jaasModule: String = JAAS_SCRAM_MODULE
    var securityProtocol: String = "SASL_PLAINTEXT"

    // экранируем кавычки/бэкслеш (на случай сложных паролей)
    private fun escapeJaas(s: String) = s.replace("\\", "\\\\").replace("\"", "\\\"")

    open fun toProperties() = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

        if (saslMechanism == "PLAIN" && jaasModule != JAAS_PLAIN_MODULE) {
            jaasModule = JAAS_PLAIN_MODULE
        } else if ((saslMechanism == "SCRAM-SHA-256" || saslMechanism == "SCRAM-SHA-512")
            && jaasModule != JAAS_SCRAM_MODULE
        ) {
            jaasModule = JAAS_SCRAM_MODULE
        }

        val u = escapeJaas(username)
        val p = escapeJaas(password)
        put("sasl.jaas.config", """$jaasModule required username="$u" password="$p";""")
        put("security.protocol", securityProtocol)
        put("sasl.mechanism", saslMechanism)
    }

    companion object {
        const val JAAS_SCRAM_MODULE = "org.apache.kafka.common.security.scram.ScramLoginModule"
        const val JAAS_PLAIN_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"

        init {
            // Критично для JDK 17/21: запрещаем попытки брать креды из Subject
            if (System.getProperty("javax.security.auth.useSubjectCredsOnly").isNullOrBlank()) {
                System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
            }
        }
    }
}

class KafkaConsumerConfig(
    bootstrapServers: String,
    username: String,
    password: String
) : KafkaConfig(bootstrapServers, username, password) {

    // делаем публичными — чтобы можно было задавать снаружи
    var groupIdPrefix: String? = null
    var groupId: String? = null
    var groupInstanceId: String? = null

    var autoCommit: Boolean = true
    var autoOffsetReset: String = "earliest"

    var partitionAssignmentStrategy: String =
        "org.apache.kafka.clients.consumer.RangeAssignor,org.apache.kafka.clients.consumer.CooperativeStickyAssignor"

    override fun toProperties(): Properties {
        return super.toProperties().apply {
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
            put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy)

            if (!autoCommit) {
                put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            }

            groupIdPrefix?.let {
                put(ConsumerConfig.GROUP_ID_CONFIG, "$it${UUID.randomUUID()}")
            } ?: groupId?.let {
                put(ConsumerConfig.GROUP_ID_CONFIG, it)
            }

            groupInstanceId?.let {
                put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, it)
            }
        }
    }
}