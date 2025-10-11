package com.validator.e2e

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.validator.app.model.ValidationPayload
import com.validator.app.model.ValidatedPayload
import consumer.service.ConsumerKafkaConfig
import consumer.service.ConsumerKafkaService
import consumer.service.runService
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import producer.service.ProducerKafkaConfig
import producer.service.ProducerKafkaService
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ValidatorServiceE2eTests {

    private val mapper: ObjectMapper = ObjectMapper()
        .registerKotlinModule()
        .registerModule(JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    private lateinit var producer: ProducerKafkaService<ValidationPayload>
    private lateinit var consumer: ConsumerKafkaService<ValidatedPayload>

    private val bootstrapServers: String = envOrDefault("VALIDATOR_KAFKA_BOOTSTRAP", "localhost:8817")
    private val inputTopic: String = envOrDefault("VALIDATOR_TOPIC_INPUT", "in_validator")
    private val outputTopic: String = envOrDefault("VALIDATOR_TOPIC_OUTPUT", "out_validator")
    private val securityProtocol: String? = envOrNull("VALIDATOR_KAFKA_SECURITY_PROTOCOL")
    private val saslMechanism: String? = envOrNull("VALIDATOR_KAFKA_SASL_MECHANISM")
    private val username: String? = envOrNull("VALIDATOR_KAFKA_USERNAME")
    private val password: String? = envOrNull("VALIDATOR_KAFKA_PASSWORD")
    private val resolvedSecurityProtocol: String = securityProtocol
        ?: if (!username.isNullOrBlank() && !password.isNullOrBlank()) "SASL_PLAINTEXT" else "PLAINTEXT"
    private val resolvedSaslMechanism: String = saslMechanism ?: "SCRAM-SHA-256"

    @BeforeAll
    fun setUp() {
        val producerConfig = ProducerKafkaConfig(
            bootstrapServers = bootstrapServers,
            username = username,
            password = password,
        ).apply {
            this.securityProtocol = resolvedSecurityProtocol
            this.saslMechanism = resolvedSaslMechanism
            clientId = "validator-e2e-tests-${UUID.randomUUID()}"
        }

        producer = ProducerKafkaService(
            cfg = producerConfig,
            topic = inputTopic,
            mapper = mapper,
        )

        val consumerConfig = ConsumerKafkaConfig(
            bootstrapServers = bootstrapServers,
            username = username,
            password = password,
        ).apply {
            this.securityProtocol = resolvedSecurityProtocol
            this.saslMechanism = resolvedSaslMechanism
            groupIdPrefix = "validator-e2e-"
            autoCommit = false
            awaitTopic = outputTopic
            awaitMapper = mapper
            awaitClazz = ValidatedPayload::class.java
            awaitLastNPerPartition = 0
        }

        consumer = runService(consumerConfig) { it.eventId }
        consumer.start()

        // небольшая пауза, чтобы консюмер успел подписаться на топик до начала теста
        Thread.sleep(500)
    }

    @AfterAll
    fun tearDown() {
        if (::producer.isInitialized) {
            producer.close()
        }
        if (::consumer.isInitialized) {
            consumer.close()
        }
    }

    @Test
    fun `payload with typeAction 100 is enriched and forwarded`() {
        val eventId = UUID.randomUUID().toString()
        val payload = ValidationPayload(
            eventId = eventId,
            userId = "user-${UUID.randomUUID()}",
            typeAction = 100,
            status = "NEW",
            sourceSystem = "validator-e2e",
            priority = 7,
            amount = BigDecimal("321.00"),
        )

        producer.send(eventId, payload)

        val records = consumer.waitForKeyList(eventId, timeoutMs = 30_000, min = 1, max = 1)
        val validated = records.first()

        assertEquals(payload.eventId, validated.eventId)
        assertEquals(payload.userId, validated.userId)
        assertEquals(payload.typeAction, validated.typeAction)
        assertEquals(payload.status, validated.status)
        assertEquals(payload.sourceSystem, validated.sourceSystem)
        assertEquals(payload.priority, validated.priority)
        assertEquals(payload.amount, validated.amount)
        assertTrue(validated.validatedAtIso.isNotBlank(), "validatedAtIso should not be blank")

        val parsedTimestamp = assertDoesNotThrow {
            OffsetDateTime.parse(validated.validatedAtIso)
        }
        assertNotNull(parsedTimestamp)
    }

    private fun envOrDefault(name: String, default: String): String =
        envOrNull(name) ?: default

    private fun envOrNull(name: String): String? =
        System.getenv(name)?.takeIf { it.isNotBlank() }
}
