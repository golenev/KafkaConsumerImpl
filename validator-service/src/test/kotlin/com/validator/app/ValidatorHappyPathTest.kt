package com.validator.app

import com.fasterxml.jackson.databind.ObjectMapper
import com.validator.app.config.ValidatorKafkaProperties
import com.validator.app.config.ValidatorTopicsProperties
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
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import producer.service.ProducerKafkaConfig
import producer.service.ProducerKafkaService
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.util.UUID

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ValidatorHappyPathTest {

    @Autowired
    private lateinit var kafkaProperties: ValidatorKafkaProperties

    @Autowired
    private lateinit var topicsProperties: ValidatorTopicsProperties

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    private lateinit var producer: ProducerKafkaService<ValidationPayload>
    private lateinit var consumer: ConsumerKafkaService<ValidatedPayload>

    @BeforeAll
    fun setUp() {
        val producerConfig = ProducerKafkaConfig(
            bootstrapServers = kafkaProperties.bootstrapServers,
            username = kafkaProperties.username,
            password = kafkaProperties.password,
        ).apply {
            securityProtocol = kafkaProperties.securityProtocol ?: "PLAINTEXT"
            kafkaProperties.saslMechanism?.let { saslMechanism = it }
            clientId = "validator-happy-path-test"
        }

        producer = ProducerKafkaService(
            cfg = producerConfig,
            topic = topicsProperties.input,
            mapper = objectMapper,
        )

        val consumerConfig = ConsumerKafkaConfig(
            bootstrapServers = kafkaProperties.bootstrapServers,
            username = kafkaProperties.username,
            password = kafkaProperties.password,
        ).apply {
            securityProtocol = kafkaProperties.securityProtocol ?: "PLAINTEXT"
            kafkaProperties.saslMechanism?.let { saslMechanism = it }
            groupIdPrefix = "validator-happy-path-"
            autoCommit = false
            awaitTopic = topicsProperties.output
            awaitMapper = objectMapper
            awaitClazz = ValidatedPayload::class.java
            awaitLastNPerPartition = 0
        }

        consumer = runService(consumerConfig) { it.eventId }
        consumer.start()
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
            sourceSystem = "integration-test",
            priority = 5,
            amount = BigDecimal("123.45"),
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
}
