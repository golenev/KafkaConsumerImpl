package consumer.service

import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.*

open class KafkaConfig(
    protected val bootstrapServers: String,
    protected val username: String?,
    protected val password: String?,
) {

    private val JAAS_SCRAM_MODULE =
        "org.apache.kafka.common.security.scram.ScramLoginModule"
    private val JAAS_PLAIN_MODULE =
        "org.apache.kafka.common.security.plain.PlainLoginModule"
    var saslMechanism: String = "SCRAM-SHA-256"
        set(value) {
            field = value
            jaasModule = when (value) {
                "PLAIN" -> JAAS_PLAIN_MODULE
                else -> JAAS_SCRAM_MODULE
            }
        }

    var jaasModule: String = JAAS_SCRAM_MODULE
    var securityProtocol: String = "PLAINTEXT"

    open fun toProperties(): Properties = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

        if (saslMechanism == "PLAIN" && jaasModule != JAAS_PLAIN_MODULE) {
            jaasModule = JAAS_PLAIN_MODULE
        } else if (
            (saslMechanism == "SCRAM-SHA-256" || saslMechanism == "SCRAM-SHA-512")
            && jaasModule != JAAS_SCRAM_MODULE
        ) {
            jaasModule = JAAS_SCRAM_MODULE
        }

        val sanitizedUsername = username?.takeUnless { it.isBlank() }
        val sanitizedPassword = password?.takeUnless { it.isBlank() }

        if (securityProtocol.uppercase().startsWith("SASL")) {
            val (user, pass) = when {
                sanitizedUsername == null && sanitizedPassword == null ->
                    throw IllegalStateException(
                        "Для security.protocol=$securityProtocol необходимо задать логин и пароль Kafka"
                    )

                sanitizedUsername == null || sanitizedPassword == null ->
                    throw IllegalStateException(
                        "Нужно одновременно указать validator.kafka.username и validator.kafka.password"
                    )

                else -> sanitizedUsername to sanitizedPassword
            }

            put(
                "sasl.jaas.config",
                """$jaasModule required username=\"${escape(user)}\" password=\"${escape(pass)}\";"""
            )
            put("sasl.mechanism", saslMechanism)
        } else if (sanitizedUsername != null || sanitizedPassword != null) {
            if (sanitizedUsername == null || sanitizedPassword == null) {
                throw IllegalStateException(
                    "Нужно одновременно указать validator.kafka.username и validator.kafka.password"
                )
            }
        }

        put("security.protocol", securityProtocol)
    }

    private fun escape(value: String): String =
        value.replace("\\", "\\\\").replace("\"", "\\\"")

}

